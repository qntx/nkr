#!/usr/bin/env python3
"""strfry write-policy fixture for the conformance suite.

The Workers fixture (`worker.js`) implements the identical rules so
both editions see the same verdicts: content `policy:reject` rejects
with `blocked: test policy`, `policy:reject-empty` rejects with an
empty msg, `policy:shadow` shadowRejects `shadowed`, and everything
else is accepted.
"""
import json
import sys

for line in sys.stdin:
    request = json.loads(line)
    event = request["event"]
    content = event.get("content", "")
    if content == "policy:reject":
        answer = {"id": event["id"], "action": "reject", "msg": "blocked: test policy"}
    elif content == "policy:reject-empty":
        answer = {"id": event["id"], "action": "reject"}
    elif content == "policy:shadow":
        answer = {"id": event["id"], "action": "shadowReject", "msg": "shadowed"}
    else:
        answer = {"id": event["id"], "action": "accept"}
    sys.stdout.write(json.dumps(answer) + "\n")
    sys.stdout.flush()
