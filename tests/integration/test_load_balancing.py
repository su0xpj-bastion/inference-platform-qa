"""Traffic path behaviour: client → proxy (NGINX semantics) → 3 replicas.

These are the failure modes that page someone at 3am: uneven spread,
buffered SSE, a dead pod still getting traffic, a drain that cuts
streams, and a retried POST that bills a customer twice.
"""
from __future__ import annotations

import threading
import time
import urllib.request
from collections import Counter

import httpx
import openai
import pytest

from inferqa.loadgen import build_report, check_slo, run_load
from inferqa.probe import stream_chat
from tests.conftest import _start_lb, http_json, spawn_replica

pytestmark = pytest.mark.integration


def served_by(tb, n: int) -> Counter:
    return Counter(stream_chat(tb.client(), "ping").served_by for _ in range(n))


def chat_requests_seen(replica) -> int:
    body = urllib.request.urlopen(f"{replica.url}/metrics", timeout=2).read().decode()
    return sum(int(line.rsplit(" ", 1)[1]) for line in body.splitlines()
               if line.startswith('mockserve_requests_total{path="/v1/chat/completions"'))


def test_round_robin_spreads_evenly(testbed):
    counts = served_by(testbed, 30)
    assert set(counts) == {"pod-0", "pod-1", "pod-2"}
    assert max(counts.values()) - min(counts.values()) <= 1


def test_sse_is_not_buffered(testbed):
    """A buffering proxy turns TTFT into total latency. Users see a frozen UI."""
    slow = spawn_replica("slow", MOCKSERVE_TBT_MS="40")
    testbed.extra.append(slow)
    lb = _start_lb([f"127.0.0.1:{slow.port}"], fail_timeout=1)
    try:
        client = openai.OpenAI(base_url=f"http://127.0.0.1:{lb.server_address[1]}/v1", api_key="t", max_retries=0)
        r = stream_chat(client, " ".join(["w"] * 20))
        assert r.timing.tokens == 20
        assert r.timing.ttft < r.timing.total * 0.3, f"ttft {r.timing.ttft:.3f}s vs total {r.timing.total:.3f}s"
    finally:
        lb.shutdown()


def test_killed_replica_gets_no_traffic(testbed):
    testbed.replicas[1].kill()
    counts = served_by(testbed, 20)
    assert "pod-1" not in counts
    assert sum(counts.values()) == 20, "connect-refused must fail over, never surface to the client"


def test_ejected_replica_returns_after_fail_timeout(testbed):
    victim = testbed.replicas[0]
    victim.kill()
    served_by(testbed, 6)                       # proxy learns pod-0 is down
    back = spawn_replica("pod-0")
    testbed.extra.append(back)
    # Re-point the proxy's peer to the new port, like Endpoints updating after a reschedule.
    ups = testbed.lb_server.RequestHandlerClass.upstreams
    ups.peers[ups.peers.index(f"127.0.0.1:{victim.port}")] = f"127.0.0.1:{back.port}"
    time.sleep(2.1)                             # fail_timeout=2s in fixture
    assert "pod-0" in served_by(testbed, 6)


def test_midstream_failure_is_not_replayed(testbed):
    """POST reached a pod, then the pod died. Retrying would double-bill; the proxy must not."""
    before = sum(chat_requests_seen(r) for r in testbed.replicas)
    with pytest.raises((openai.APIError, httpx.HTTPError)):
        stream_chat(testbed.client(), "a b c d e f g h", extra_headers={"x-fault": "drop-midstream"})
    after = sum(chat_requests_seen(r) for r in testbed.replicas)
    assert after - before == 1


def test_sigterm_drains_inflight_stream(testbed):
    """Rolling deploy: pod gets SIGTERM mid-stream. The stream must finish; new work must go elsewhere."""
    slow = spawn_replica("draining", MOCKSERVE_TBT_MS="30")
    testbed.extra.append(slow)
    client = openai.OpenAI(base_url=f"{slow.url}/v1", api_key="t", max_retries=0)
    result = {}

    def run():
        result["r"] = stream_chat(client, " ".join(["tok"] * 30))

    t = threading.Thread(target=run)
    t.start()
    time.sleep(0.2)
    slow.term()
    time.sleep(0.1)
    assert http_json("GET", f"{slow.url}/readyz")[0] == 503
    t.join(10)
    r = result["r"]
    assert r.ok and r.timing.tokens == 30 and r.finish_reason == "stop"
    slow.proc.wait(5)
    assert slow.proc.returncode == 0


def test_not_ready_replica_refuses_admin_toggle(testbed):
    pod = testbed.replicas[2]
    assert http_json("POST", f"{pod.url}/admin/ready", {"ready": False})[0] == 200
    assert http_json("GET", f"{pod.url}/readyz")[0] == 503
    # Liveness stays green: a not-ready pod must not be restarted, only taken out of rotation.
    assert http_json("GET", f"{pod.url}/healthz")[0] == 200


def test_502_when_every_upstream_is_dead(testbed):
    for r in testbed.replicas:
        r.kill()
    status, _, body = http_json("POST", f"{testbed.lb_url}/v1/chat/completions",
                                {"model": "llama3.1-8b", "messages": [{"role": "user", "content": "x"}]})
    assert status == 502 and body["error"]["type"] == "bad_gateway"


def test_load_meets_slo(testbed):
    report = build_report(run_load(testbed.client(), n=60, concurrency=8))
    slo = {"max_error_rate": 0.0, "ttft_s": {"p99": 1.0}, "tbt_s": {"p99": 0.05},
           "min_output_tps_p50": 20, "min_replicas_serving": 3}
    assert check_slo(report, slo) == [], report
