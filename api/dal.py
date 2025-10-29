import time
from typing import Optional, Dict, Any
from common.aws import ddb_table
from common.config import DDB_JOBS_TABLE

_table = ddb_table(DDB_JOBS_TABLE)

def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def put_job(item: Dict[str, Any]):
    item.setdefault("createdAt", now_iso())
    _table.put_item(Item=item)

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    resp = _table.get_item(Key={"jobId": job_id})
    return resp.get("Item")

def update_job(job_id: str, fields: Dict[str, Any]):
    fields = dict(fields)  # avoid mutating caller dict
    fields["updatedAt"] = now_iso()

    expr_items = []
    names: Dict[str, str] = {}
    values: Dict[str, Any] = {}

    # Build SET expression placeholders
    for i, (k, v) in enumerate(fields.items(), start=1):
        nk = f"#k{i}"
        vk = f":v{i}"
        names[nk] = k
        values[vk] = v
        expr_items.append(f"{nk} = {vk}")

    update_expr = "SET " + ", ".join(expr_items)

    # Merge condition placeholders
    names["#st"] = "status"
    values[":done"] = "DONE"

    # If item exists and is already DONE, do not overwrite it
    return _table.update_item(
        Key={"jobId": job_id},
        UpdateExpression=update_expr,
        ConditionExpression="attribute_not_exists(jobId) OR #st <> :done",
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
        ReturnValues="ALL_NEW",
    )
