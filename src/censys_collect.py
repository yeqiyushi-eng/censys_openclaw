import os
import sys
import csv
import json
import time
import argparse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Iterable, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo

from censys.search import CensysHosts


# -------------------------
# 設定（デフォルト）
# -------------------------
DEFAULT_QUERY = '(host.services.endpoints.http.html_title:{"Moltbot Control", "clawdbot Control"}) and host.location.country = "Japan"'
DEFAULT_TITLES = ["Moltbot Control", "clawdbot Control"]
DEFAULT_PER_PAGE = 100  # Censys SDKのデフォルトも 100 :contentReference[oaicite:2]{index=2}
DEFAULT_MAX_PAGES = 0   # 0 = 制限なし（レート制限等で止まるまで）
DEFAULT_SLEEP_SEC = 0.2

# 出力ディレクトリ
OUT_DIR = "out"


@dataclass
class Counters:
    pages: int = 0
    hosts: int = 0
    rows: int = 0


def _now_jst_date_str() -> str:
    jst = ZoneInfo("Asia/Tokyo")
    return datetime.now(jst).strftime("%Y-%m-%d")


def _safe_get(d: Dict[str, Any], path: str, default=None):
    """
    path: "a.b.c" のようなドット区切り
    """
    cur: Any = d
    for key in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


def _iter_services(host_doc: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    services = host_doc.get("services", [])
    if isinstance(services, list):
        for s in services:
            if isinstance(s, dict):
                yield s


def _iter_endpoints(service_doc: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    endpoints = service_doc.get("endpoints", [])
    if isinstance(endpoints, list):
        for ep in endpoints:
            if isinstance(ep, dict):
                yield ep


def _http_from_endpoint(endpoint_doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    http = endpoint_doc.get("http")
    return http if isinstance(http, dict) else None


def build_rows_from_host(
    host_doc: Dict[str, Any],
    match_titles: List[str],
) -> List[Dict[str, Any]]:
    """
    1 host JSON から、HTTP endpoint 単位の行を作る（マッチtitleのみ抽出）
    """
    rows: List[Dict[str, Any]] = []

    ip = host_doc.get("ip")
    country = _safe_get(host_doc, "location.country")
    province = _safe_get(host_doc, "location.province")
    city = _safe_get(host_doc, "location.city")
    postal_code = _safe_get(host_doc, "location.postal_code")
    lat = _safe_get(host_doc, "location.latitude")
    lon = _safe_get(host_doc, "location.longitude")

    asn = _safe_get(host_doc, "autonomous_system.asn")
    as_name = _safe_get(host_doc, "autonomous_system.name")

    for svc in _iter_services(host_doc):
        port = svc.get("port")
        service_name = svc.get("service_name")
        transport = svc.get("transport_protocol")

        # software は list のことが多いので、代表的な先頭だけ拾う（空なら None）
        sw_product = None
        sw_vendor = None
        sw_version = None
        software = svc.get("software", [])
        if isinstance(software, list) and software:
            first = software[0] if isinstance(software[0], dict) else None
            if first:
                sw_product = first.get("product")
                sw_vendor = first.get("vendor")
                sw_version = first.get("version")

        for ep in _iter_endpoints(svc):
            http = _http_from_endpoint(ep)
            if not http:
                continue

            html_title = http.get("html_title")
            if html_title not in match_titles:
                continue

            # よく使うHTTP情報
            status_code = http.get("status_code")
            host = http.get("host")
            path = http.get("path")
            scheme = http.get("scheme")

            rows.append(
                {
                    "ip": ip,
                    "country": country,
                    "province": province,
                    "city": city,
                    "postal_code": postal_code,
                    "latitude": lat,
                    "longitude": lon,
                    "asn": asn,
                    "as_name": as_name,
                    "port": port,
                    "service_name": service_name,
                    "transport_protocol": transport,
                    "software_product": sw_product,
                    "software_vendor": sw_vendor,
                    "software_version": sw_version,
                    "http_scheme": scheme,
                    "http_host": host,
                    "http_path": path,
                    "http_status_code": status_code,
                    "http_html_title": html_title,
                }
            )

    return rows


def write_jsonl(path: str, host_docs: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for doc in host_docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        # ヘッダだけ作る（空結果でも分析しやすい）
        header = [
            "ip", "country", "province", "city", "postal_code", "latitude", "longitude",
            "asn", "as_name",
            "port", "service_name", "transport_protocol",
            "software_product", "software_vendor", "software_version",
            "http_scheme", "http_host", "http_path", "http_status_code", "http_html_title",
        ]
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
        return

    header = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def censys_collect(
    query: str,
    titles: List[str],
    per_page: int,
    max_pages: int,
    sleep_sec: float,
) -> Tuple[Counters, str, str]:
    """
    戻り値: (counters, jsonl_path, csv_path)
    """
    api_id = os.getenv("CENSYS_API_ID")
    api_secret = os.getenv("CENSYS_API_SECRET")
    if not api_id or not api_secret:
        raise RuntimeError("Missing env vars: CENSYS_API_ID / CENSYS_API_SECRET (set GitHub Secrets).")

    # JST日付でファイル名に付与
    date_str = _now_jst_date_str()
    base = f"censys_hosts_jp_moltbot_clawdbot_{date_str}"
    jsonl_path = os.path.join(OUT_DIR, f"{base}.jsonl")
    csv_path = os.path.join(OUT_DIR, f"{base}.csv")

    # 取得したHostドキュメント（必要最小限にするため fields 指定推奨）
    # fields を絞るとレスポンスが軽くなり、上限に当たりにくい
    fields = [
        "ip",
        "location.country",
        "location.province",
        "location.city",
        "location.postal_code",
        "location.latitude",
        "location.longitude",
        "autonomous_system.asn",
        "autonomous_system.name",
        "services.port",
        "services.service_name",
        "services.transport_protocol",
        "services.software.product",
        "services.software.vendor",
        "services.software.version",
        "services.endpoints.http.html_title",
        "services.endpoints.http.status_code",
        "services.endpoints.http.host",
        "services.endpoints.http.path",
        "services.endpoints.http.scheme",
    ]

    h = CensysHosts(api_id=api_id, api_secret=api_secret)

    cursor: Optional[str] = None
    counters = Counters()
    all_host_docs: List[Dict[str, Any]] = []
    all_rows: List[Dict[str, Any]] = []

    def should_continue() -> bool:
        if max_pages and counters.pages >= max_pages:
            return False
        return True

    while should_continue():
        try:
            # Censys SDK: h.search(query, per_page=100, cursor=..., pages=1, fields=...) :contentReference[oaicite:3]{index=3}
            page_iter = h.search(
                query=query,
                per_page=per_page,
                cursor=cursor,
                pages=1,
                fields=fields,
            )

            # pages=1 なので 1ページ分だけ iterable で返る想定
            page_docs: Optional[List[Dict[str, Any]]] = None
            for docs in page_iter:
                page_docs = docs
                break

            if not page_docs:
                break

            counters.pages += 1
            counters.hosts += len(page_docs)

            for host_doc in page_docs:
                all_host_docs.append(host_doc)
                rows = build_rows_from_host(host_doc, titles)
                all_rows.extend(rows)

            # 次ページ用の cursor を SDK内部から取れないので、
            # 同じ query+cursor を使うには raw response が必要…と思いがちですが、
            # SDK の Query オブジェクトは内部でカーソルを進める設計です。
            # ただし pages=1 で止めると次に進まないため、
            # ここでは「cursor を使わず pages を増やす」方式に切り替えます。
            #
            # → 安全のため「cursor 手動」ではなく「pages=-1（全ページ）に近い挙動」を実装するため、
            #    以降は pages=1 をやめて、1回の search 呼び出しで複数ページ取得します。
            #
            # ただ、無制限取得はAPI上限に当たりやすいので、ここでループを抜けて
            # 「バッチ方式（複数ページまとめて）」に移行します。
            break

        except Exception as e:
            # レート制限/上限/一時障害などでも「取れた分は保存」したいので終了
            print(f"[WARN] Stopped due to error on page {counters.pages + 1}: {e}", file=sys.stderr)
            break

        finally:
            if sleep_sec > 0:
                time.sleep(sleep_sec)

    # ここから「まとめて複数ページ」方式で続行（SDKの内部カーソルに任せる）
    # すでに 1ページ取っている可能性があるので、残りを max_pages に合わせて取得する
    remaining_pages = 0
    if max_pages == 0:
        remaining_pages = -1  # 無制限（SDKが止まるまで）
    else:
        remaining_pages = max_pages - counters.pages
        if remaining_pages <= 0:
            remaining_pages = 0

    if remaining_pages != 0:
        try:
            page_iter = h.search(
                query=query,
                per_page=per_page,
                pages=remaining_pages,
                fields=fields,
            )
            for page_docs in page_iter:
                if not page_docs:
                    break
                counters.pages += 1
                counters.hosts += len(page_docs)

                for host_doc in page_docs:
                    all_host_docs.append(host_doc)
                    rows = build_rows_from_host(host_doc, titles)
                    all_rows.extend(rows)

                if max_pages and counters.pages >= max_pages:
                    break

                if sleep_sec > 0:
                    time.sleep(sleep_sec)

        except Exception as e:
            print(f"[WARN] Stopped due to error after {counters.pages} pages: {e}", file=sys.stderr)

    counters.rows = len(all_rows)

    # 保存（途中まででもOK）
    write_jsonl(jsonl_path, all_host_docs)
    write_csv(csv_path, all_rows)

    return counters, jsonl_path, csv_path


def main():
    p = argparse.ArgumentParser(description="Collect Censys hosts search results and export JSONL/CSV.")
    p.add_argument("--query", default=DEFAULT_QUERY, help="Censys query (CenQL).")
    p.add_argument("--titles", default=",".join(DEFAULT_TITLES), help="Comma-separated HTML titles to match.")
    p.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE, help="Results per page (max typically 100).")
    p.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES, help="0=unlimited, else stop after N pages.")
    p.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_SEC, help="Sleep seconds between pages.")
    args = p.parse_args()

    titles = [t.strip() for t in args.titles.split(",") if t.strip()]
    counters, jsonl_path, csv_path = censys_collect(
        query=args.query,
        titles=titles,
        per_page=args.per_page,
        max_pages=args.max_pages,
        sleep_sec=args.sleep,
    )

    print("==== DONE ====")
    print(f"pages: {counters.pages}")
    print(f"hosts: {counters.hosts}")
    print(f"rows (matched endpoints): {counters.rows}")
    print(f"jsonl: {jsonl_path}")
    print(f"csv : {csv_path}")


if __name__ == "__main__":
    main()
