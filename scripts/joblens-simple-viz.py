#!/usr/bin/env python3
"""
joblens-simple-viz — Real-time visualization CLI for JobLens monitoring data.
===========================================================

Fetches CPU, memory, I/O, and network data for a specified job from
Elasticsearch, generates an interactive HTML dashboard with Plotly,
and starts a local HTTP server with auto-refresh for live monitoring.

Usage:
  export ES_USERNAME=your_username
  export ES_PASSWORD=your_password
  python tools/joblens_viz.py 12345.0
  python tools/joblens_viz.py 12345.0 --cluster ihep_slurm --refresh 15 --port 9000
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("joblens-simple-viz")

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

# 集群名称 → index_prefix 映射
CLUSTER_PREFIXES = {
    "condor": "collector",
}

DEFAULT_CLUSTER = "condor"
DEFAULT_REFRESH = 10  # 秒
DEFAULT_PORT = 8765

ES_INDEX_TYPES = ["cpumem_collector", "io_collector", "net_collector"]


# ---------------------------------------------------------------------------
# ES 连接 & 查询
# ---------------------------------------------------------------------------

def _get_es_config() -> dict[str, Any]:
    """Read ES connection config from environment variables."""
    return {
        "host": os.environ.get("ES_HOST", ""),
        "port": int(os.environ.get("ES_PORT", "443")),
        "scheme": os.environ.get("ES_SCHEME", "https"),
        "username": os.environ.get("ES_USERNAME", ""),
        "password": os.environ.get("ES_PASSWORD", ""),
    }


def build_es_client():
    """Create an Elasticsearch client (lazy import to avoid crashing if not installed)."""
    try:
        from elasticsearch import Elasticsearch
    except ImportError:
        print(
            "Error: elasticsearch package is required.\n"
            "  pip install elasticsearch>=8.0",
            file=sys.stderr,
        )
        sys.exit(1)

    cfg = _get_es_config()
    es_url = f"{cfg['scheme']}://{cfg['host']}:{cfg['port']}"

    params: dict[str, Any] = {
        "hosts": [es_url],
        "request_timeout": 30,
        "max_retries": 3,
        "retry_on_timeout": True,
    }
    if cfg["username"] and cfg["password"]:
        params["basic_auth"] = (cfg["username"], cfg["password"])

    client = Elasticsearch(**params)
    if not client.ping():
        log.error("Cannot connect to Elasticsearch: %s", es_url)
        sys.exit(1)
    log.info("Connected to Elasticsearch: %s", es_url)
    return client


def parse_jobid(jobid: str) -> int:
    """Convert jobid to the integer format stored in ES.
    "12345.0" (HTCondor) → 123450
    "123456"   (Slurm)   → 123456
    """
    if "." in jobid:
        parts = jobid.split(".")
        return int(parts[0] + parts[-1])
    return int(jobid)


def query_index(client, index_pattern: str, jobid_int: int,
               hours: int = 2) -> list[dict]:
    """Query a single ES index and return the _source of matching documents.

    hours: Only query data from the last N hours (default 2h),
           set to 0 for unlimited time range.
    """
    must_clause = [{"term": {"job_info.JobID": jobid_int}}]
    if hours > 0:
        must_clause.append(
            {"range": {"@timestamp": {"gte": f"now-{hours}h"}}}
        )
    query = {
        "query": {"bool": {"must": must_clause}},
        "sort": [{"@timestamp": {"order": "asc"}}],
        "size": 2000,
    }
    try:
        resp = client.search(index=index_pattern, body=query)
    except Exception as exc:
        log.warning("Index %s query failed: %s", index_pattern, exc)
        return []

    hits = resp.get("hits", {}).get("hits", [])
    total = resp.get("hits", {}).get("total", {}).get("value", len(hits))
    sources = [h["_source"] for h in hits]

    # If results exceed 10000, use auto_date_histogram for downsampling
    if total > 10000:
        log.info("Index %s returned %s docs > 10000, performing downsampling", index_pattern, total)
        agg_query = {
            "query": {"bool": {"must": [{"term": {"job_info.JobID": jobid_int}}]}},
            "size": 0,
            "aggs": {
                "time_buckets": {
                    "auto_date_histogram": {
                        "field": "@timestamp",
                        "buckets": 2000,
                    },
                    "aggs": {
                        "sample": {
                            "top_hits": {"size": 1, "sort": [{"@timestamp": "asc"}]}
                        }
                    },
                }
            },
        }
        try:
            agg_resp = client.search(index=index_pattern, body=agg_query)
            buckets = (
                agg_resp.get("aggregations", {})
                .get("time_buckets", {})
                .get("buckets", [])
            )
            sources = [
                b["sample"]["hits"]["hits"][0]["_source"]
                for b in buckets
                if b.get("sample", {}).get("hits", {}).get("hits")
            ]
            log.info("Downsampling complete, %s sample points", len(sources))
        except Exception as exc:
            log.warning("Downsampling query failed, using original %s docs: %s", len(sources), exc)

    return sources


def fetch_all_data(client, prefix: str, jobid_int: int,
                   hours: int = 2) -> dict[str, list[dict]]:
    """Query all index types in parallel."""
    result = {}
    for idx_type in ES_INDEX_TYPES:
        pattern = f"{prefix}_{idx_type}*"
        log.info("Querying index: %s", pattern)
        result[idx_type] = query_index(client, pattern, jobid_int, hours=hours)
        log.info("  Got %s docs", len(result[idx_type]))
    return result


# ---------------------------------------------------------------------------
# 数据解析 → 结构化 dict
# ---------------------------------------------------------------------------

def parse_cpumem(sources: list[dict]) -> dict:
    """Parse cpumem data → {timestamp, cpu_percent, mem_mb}."""
    timestamps, cpu_vals, mem_mb_vals = [], [], []
    for doc in sources:
        ts = doc.get("@timestamp", "")
        summary = doc.get("data", {}).get("summary", {})
        cpu = summary.get("cpuPercent")
        mem_kb = summary.get("mem_rss_kb")
        timestamps.append(ts)
        cpu_vals.append(cpu if cpu is not None else 0)
        mem_mb_vals.append(mem_kb / 1024 if mem_kb is not None else 0)
    return {"timestamp": timestamps, "cpu_percent": cpu_vals, "mem_mb": mem_mb_vals}


def parse_io(sources: list[dict]) -> dict:
    """Parse io data → {timestamp, read_speed, write_speed}."""
    timestamps, read_vals, write_vals = [], [], []
    for doc in sources:
        ts = doc.get("@timestamp", "")
        summary = doc.get("data", {}).get("summary", {})
        read_vals.append(summary.get("read_speed", 0) or 0)
        write_vals.append(summary.get("write_speed", 0) or 0)
        timestamps.append(ts)
    return {"timestamp": timestamps, "read_speed": read_vals, "write_speed": write_vals}


def parse_net(sources: list[dict]) -> dict:
    """Parse net data → {timestamp, recv_rate, send_rate}."""
    timestamps, recv_vals, send_vals = [], [], []
    for doc in sources:
        ts = doc.get("@timestamp", "")
        connections = doc.get("data", {}).get("summary", {}).get("connections", [])
        if connections:
            conn = connections[0]
            recv_vals.append(conn.get("recv_rate", 0) or 0)
            send_vals.append(conn.get("send_rate", 0) or 0)
        else:
            recv_vals.append(0)
            send_vals.append(0)
        timestamps.append(ts)
    return {
        "timestamp": timestamps,
        "recv_rate": recv_vals,
        "send_rate": send_vals,
    }


# ---------------------------------------------------------------------------
# Plotly 图表生成
# ---------------------------------------------------------------------------

def _gen_html(cpumem: dict, io_data: dict, net_data: dict,
              jobid: str, cluster: str, refresh: int,
              fetch_time: float) -> str:
    """Generate the complete HTML dashboard."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        print(
            "Error: plotly package is required.\n"
            "  pip install plotly",
            file=sys.stderr,
        )
        sys.exit(1)

    # 颜色方案（深色主题）
    colors = {
        "bg": "#1a1d23",
        "paper": "#232731",
        "grid": "#333842",
        "text": "#e0e2e6",
        "cpu": "#4fc3f7",
        "mem": "#81c784",
        "read": "#4dd0e1",
        "write": "#ffb74d",
        "recv": "#ba68c8",
        "send": "#f06292",
    }

    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "CPU Usage (%)",
            "Memory Usage (MB)",
            "I/O Speed (MB/s)",
            "Network Rate (MB/s)",
        ),
        vertical_spacing=0.12,
        horizontal_spacing=0.08,
    )

    # --- CPU ---
    has_cpu = any(v > 0 for v in cpumem["cpu_percent"])
    if has_cpu:
        fig.add_trace(
            go.Scatter(
                x=cpumem["timestamp"],
                y=cpumem["cpu_percent"],
                mode="lines+markers",
                name="CPU %",
                line=dict(color=colors["cpu"], width=2),
                marker=dict(size=3),
                hovertemplate="%{x|%H:%M:%S}<br>CPU: %{y:.1f}%<extra></extra>",
            ),
            row=1, col=1,
        )

    # --- Memory ---
    has_mem = any(v > 0 for v in cpumem["mem_mb"])
    if has_mem:
        fig.add_trace(
            go.Scatter(
                x=cpumem["timestamp"],
                y=cpumem["mem_mb"],
                mode="lines+markers",
                name="Memory",
                line=dict(color=colors["mem"], width=2),
                marker=dict(size=3),
                hovertemplate="%{x|%H:%M:%S}<br>Mem: %{y:.1f} MB<extra></extra>",
            ),
            row=1, col=2,
        )

    # --- I/O ---
    has_io = any(v > 0 for v in io_data["read_speed"]) or any(
        v > 0 for v in io_data["write_speed"]
    )
    if has_io:
        # 转换为 MB/s
        read_mb = [v / (1024 * 1024) for v in io_data["read_speed"]]
        write_mb = [v / (1024 * 1024) for v in io_data["write_speed"]]
        fig.add_trace(
            go.Scatter(
                x=io_data["timestamp"],
                y=read_mb,
                mode="lines+markers",
                name="Read",
                line=dict(color=colors["read"], width=2),
                marker=dict(size=3),
                hovertemplate="%{x|%H:%M:%S}<br>Read: %{y:.2f} MB/s<extra></extra>",
            ),
            row=2, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=io_data["timestamp"],
                y=write_mb,
                mode="lines+markers",
                name="Write",
                line=dict(color=colors["write"], width=2),
                marker=dict(size=3),
                hovertemplate="%{x|%H:%M:%S}<br>Write: %{y:.2f} MB/s<extra></extra>",
            ),
            row=2, col=1,
        )

    # --- Network ---
    has_net = any(v > 0 for v in net_data["recv_rate"]) or any(
        v > 0 for v in net_data["send_rate"]
    )
    if has_net:
        # 转换为 MB/s
        recv_mb = [v / (1024 * 1024) for v in net_data["recv_rate"]]
        send_mb = [v / (1024 * 1024) for v in net_data["send_rate"]]
        fig.add_trace(
            go.Scatter(
                x=net_data["timestamp"],
                y=recv_mb,
                mode="lines+markers",
                name="Recv",
                line=dict(color=colors["recv"], width=2),
                marker=dict(size=3),
                hovertemplate="%{x|%H:%M:%S}<br>Recv: %{y:.2f} MB/s<extra></extra>",
            ),
            row=2, col=2,
        )
        fig.add_trace(
            go.Scatter(
                x=net_data["timestamp"],
                y=send_mb,
                mode="lines+markers",
                name="Send",
                line=dict(color=colors["send"], width=2),
                marker=dict(size=3),
                hovertemplate="%{x|%H:%M:%S}<br>Send: %{y:.2f} MB/s<extra></extra>",
            ),
            row=2, col=2,
        )

    # 更新布局
    fig.update_layout(
        title=dict(
            text=f"JobLens Monitor — Job {jobid} ({cluster})",
            font=dict(size=22, color=colors["text"]),
            x=0.5,
            xanchor="center",
        ),
        template="plotly_dark",
        plot_bgcolor=colors["bg"],
        paper_bgcolor=colors["paper"],
        font=dict(color=colors["text"], size=12),
        hovermode="x unified",
        margin=dict(l=50, r=30, t=80, b=60),
        height=750,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            bgcolor="rgba(0,0,0,0)",
        ),
    )

    # 更新坐标轴
    fig.update_xaxes(
        gridcolor=colors["grid"],
        zerolinecolor=colors["grid"],
        showgrid=True,
    )
    fig.update_yaxes(
        gridcolor=colors["grid"],
        zerolinecolor=colors["grid"],
        showgrid=True,
    )

    # 对空子图显示 "No Data" 标注
    if not has_cpu:
        fig.add_annotation(
            text="No Data", xref="paper", yref="paper",
            x=0.25, y=0.75, showarrow=False,
            font=dict(size=18, color="#666"),
        )
    if not has_mem:
        fig.add_annotation(
            text="No Data", xref="paper", yref="paper",
            x=0.75, y=0.75, showarrow=False,
            font=dict(size=18, color="#666"),
        )
    if not has_io:
        fig.add_annotation(
            text="No Data", xref="paper", yref="paper",
            x=0.25, y=0.25, showarrow=False,
            font=dict(size=18, color="#666"),
        )
    if not has_net:
        fig.add_annotation(
            text="No Data", xref="paper", yref="paper",
            x=0.75, y=0.25, showarrow=False,
            font=dict(size=18, color="#666"),
        )

    plotly_html = fig.to_html(
        include_plotlyjs="cdn",
        full_html=False,
        div_id="dashboard",
    )

    last_updated = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(fetch_time))

    # 组装完整 HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="{refresh}">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>JobLens Viz — {jobid}</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    background: #1a1d23;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
                 "Helvetica Neue", Arial, sans-serif;
    color: #e0e2e6;
    padding: 0;
  }}
  .container {{
    max-width: 1400px;
    margin: 0 auto;
    padding: 16px 20px;
  }}
  .header {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px 0;
    border-bottom: 1px solid #333842;
    margin-bottom: 16px;
  }}
  .header h1 {{
    font-size: 20px;
    font-weight: 600;
    color: #e0e2e6;
  }}
  .header .status {{
    font-size: 13px;
    color: #888;
  }}
  .header .status .dot {{
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 6px;
    background: #4fc3f7;
    animation: pulse 2s infinite;
  }}
  @keyframes pulse {{
    0%, 100% {{ opacity: 1; }}
    50% {{ opacity: 0.4; }}
  }}
  .footer {{
    text-align: center;
    padding: 20px 0 10px;
    font-size: 12px;
    color: #555;
    border-top: 1px solid #333842;
    margin-top: 16px;
  }}
  .footer strong {{ color: #888; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>JobLens Monitor &mdash; {jobid}</h1>
    <div class="status">
      <span class="dot"></span>
      Auto-refresh: {refresh}s &nbsp;|&nbsp; Last updated: <strong>{last_updated}</strong>
    </div>
  </div>
  {plotly_html}
  <div class="footer">
    <strong>joblens-simple-viz</strong> &mdash; Cluster: {cluster}
    &nbsp;|&nbsp; Press <kbd>Ctrl+C</kbd> to stop
  </div>
</div>
</body>
</html>"""
    return html


# ---------------------------------------------------------------------------
# HTTP 服务
# ---------------------------------------------------------------------------

class DashboardHandler(BaseHTTPRequestHandler):
    """Serves cached dashboard HTML, refreshes data at the configured interval."""

    # Store references at class level for the handler
    _state: dict = {
        "cached_html": "",
        "last_fetch": 0.0,
    }

    # ------------------------------------------------------------------
    def _refresh_data(self) -> None:
        """Fetch ES data and regenerate HTML, updating the cache."""
        state = self._state
        fetch_start = time.time()

        client = state["client"]
        prefix = state["prefix"]
        jobid_int = state["jobid_int"]
        hours = state.get("hours", 2)

        data = fetch_all_data(client, prefix, jobid_int, hours=hours)

        cpumem = parse_cpumem(data["cpumem_collector"])
        io_data = parse_io(data["io_collector"])
        net_data = parse_net(data["net_collector"])

        fetch_time = time.time()
        log.info(
            "Data fetch complete  cpu=%d  io=%d  net=%d  (%.1fs)",
            len(cpumem["timestamp"]),
            len(io_data["timestamp"]),
            len(net_data["timestamp"]),
            fetch_time - fetch_start,
        )

        html = _gen_html(
            cpumem, io_data, net_data,
            state["jobid"], state["cluster"],
            state["refresh"], fetch_time,
        )

        state["cached_html"] = html
        state["last_fetch"] = fetch_time

    # ------------------------------------------------------------------
    def _send_html(self, html: str) -> None:
        """Send the HTML response, gracefully handling client disconnection."""
        encoded = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.end_headers()
        try:
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionError):
            # 客户端提前断开连接（浏览器关闭/刷新），忽略
            pass

    # ------------------------------------------------------------------
    def do_GET(self):  # noqa: N802
        if self.path != "/":
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"404 Not Found")
            return

        state = self._state
        now = time.time()
        refresh = state.get("refresh", 10)

        # 缓存过期：距上次拉取超过 refresh 秒，重新拉取数据
        if now - state.get("last_fetch", 0) >= refresh:
            try:
                self._refresh_data()
            except Exception as exc:
                log.error("Failed to refresh data: %s", exc)
                # 如果缓存中有旧 HTML，继续使用；否则生成错误页
                if not state.get("cached_html"):
                    state["cached_html"] = (
                        "<html><body><h1>Error</h1>"
                        f"<pre>{exc}</pre>"
                        "<p>Page will retry in 10 seconds...</p>"
                        '<meta http-equiv="refresh" content="10">'
                        "</body></html>"
                    )

        self._send_html(state["cached_html"])

    # ------------------------------------------------------------------
    def log_message(self, fmt, *args):
        """Silence HTTP log output, use our logger instead."""
        log.debug("HTTP: " + fmt, *args)


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Real-time visualization for JobLens monitoring data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Environment variables:\n"
            "  ES_HOST         Elasticsearch host (default: omat4htc-es.ihep.ac.cn)\n"
            "  ES_PORT         Elasticsearch port (default: 443)\n"
            "  ES_SCHEME       Protocol (default: https)\n"
            "  ES_USERNAME     ES username\n"
            "  ES_PASSWORD     ES password\n"
            "\n"
            "Examples:\n"
            "  export ES_USERNAME=readonly\n"
            "  export ES_PASSWORD=your_password\n"
            "  python joblens-simple-viz.py 12345.0\n"
            "  python joblens-simple-viz.py 12345.0 --cluster ihep_slurm --refresh 15\n"
            "  python joblens-simple-viz.py 12345.0 --hours 12\n"
        ),
    )
    parser.add_argument("jobid", help='Job ID (e.g. "12345.0" or "123456")')
    parser.add_argument(
        "--cluster",
        default=DEFAULT_CLUSTER,
        choices=list(CLUSTER_PREFIXES.keys()),
        help=f"Cluster name (default: {DEFAULT_CLUSTER})",
    )
    parser.add_argument(
        "--refresh",
        type=int,
        default=DEFAULT_REFRESH,
        metavar="SEC",
        help=f"Page auto-refresh interval in seconds (default: {DEFAULT_REFRESH})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        metavar="PORT",
        help=f"Local HTTP port (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=2,
        metavar="N",
        help="Query data from the last N hours (default: 2, set to 0 for unlimited)",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not auto-open browser",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    jobid = args.jobid
    cluster = args.cluster
    refresh = args.refresh
    port = args.port
    hours = args.hours
    no_browser = args.no_browser
    prefix = CLUSTER_PREFIXES[cluster]

    # Check required environment variables
    if not os.environ.get("ES_USERNAME"):
        log.warning("Environment variable ES_USERNAME is not set")
    if not os.environ.get("ES_PASSWORD"):
        log.warning("Environment variable ES_PASSWORD is not set")
    if not os.environ.get("ES_USERNAME") and not os.environ.get("ES_PASSWORD"):
        log.warning("Neither ES_USERNAME nor ES_PASSWORD is set, attempting unauthenticated connection")

    jobid_int = parse_jobid(jobid)
    log.info("Job ID: %s  →  ES int: %d", jobid, jobid_int)
    log.info("Cluster: %s  →  index_prefix: %s", cluster, prefix)
    log.info("Refresh interval: %ds", refresh)
    log.info("Time window: last %s hour(s)", str(hours) if hours > 0 else "unlimited")

    # 连接 ES
    client = build_es_client()

    # ---------------------------------------------------------------
    # Pre-fetch data + pre-generate HTML (avoid wait on first request)
    # ---------------------------------------------------------------
    log.info("Initial data fetch in progress...")
    fetch_start = time.time()
    data = fetch_all_data(client, prefix, jobid_int, hours=hours)

    cpumem = parse_cpumem(data["cpumem_collector"])
    io_data = parse_io(data["io_collector"])
    net_data = parse_net(data["net_collector"])

    fetch_time = time.time()
    log.info(
        "Data fetch complete  cpu=%d  io=%d  net=%d  (%.1fs)",
        len(cpumem["timestamp"]),
        len(io_data["timestamp"]),
        len(net_data["timestamp"]),
        fetch_time - fetch_start,
    )

    cached_html = _gen_html(
        cpumem, io_data, net_data,
        jobid, cluster, refresh, fetch_time,
    )
    # ---------------------------------------------------------------

    # 初始化 handler 状态（含缓存 HTML）
    DashboardHandler._state = {
        "client": client,
        "jobid": jobid,
        "jobid_int": jobid_int,
        "cluster": cluster,
        "prefix": prefix,
        "refresh": refresh,
        "hours": hours,
        "cached_html": cached_html,
        "last_fetch": fetch_time,
    }

    # 启动 HTTP 服务
    server = HTTPServer(("0.0.0.0", port), DashboardHandler)
    url = f"http://localhost:{port}"

    print()
    print(f"  Dashboard URL: {url}")
    print(f"  Auto-refresh every {refresh}s")
    print(f"  Press Ctrl+C to stop")
    print()

    if not no_browser:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print()
        log.info("Received Ctrl+C, shutting down...")
    finally:
        server.server_close()
        log.info("Server stopped")


if __name__ == "__main__":
    main()
