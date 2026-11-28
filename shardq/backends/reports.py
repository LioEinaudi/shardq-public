"""Read frozen backend report fields, including jobs registered before report hooks."""


def report_for_job(spec: dict) -> dict:
    if "backend_report" in spec:
        return spec["backend_report"]
    # Original schema-1 writers exported these evaluation fields for every job.
    from .lmms_reports import LmmsReports

    return LmmsReports.report_metadata(spec)


def attempt_report(extra: dict) -> dict:
    """Preserve the v1 raw artifact alias alongside namespaced adapter data."""
    return {"backend_extra": extra, "raw_results": extra.get("raw_results")}
