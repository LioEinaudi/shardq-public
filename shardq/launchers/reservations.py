"""Read and write submission resource reservations across ledger versions."""


def reservation_fields(pool, admission):
    fields = {"capacity_units": admission.units_per_worker, "capacity_scope": list(admission.scope)}
    # Preserve the original audit fields for consumers of built-in cloud records.
    if pool.launcher in {"volc", "aihc"}:
        fields["gpu_slots"] = admission.units_per_worker
    if pool.launcher == "volc":
        fields.update(resource_queue=pool.launcher_args.get("queue"),
                      resource_creator=pool.launcher_args.get("creator"))
    return fields


def read_reservation(row, default_units):
    if "capacity_scope" in row:
        return tuple(row["capacity_scope"]), row["capacity_units"]
    scope = ()
    if row["launcher"] == "volc":
        scope = ("volc", str(row.get("resource_queue") or ""))
        if row.get("resource_creator") is not None:
            scope += (str(row["resource_creator"]),)
    return scope, row.get("gpu_slots", default_units)
