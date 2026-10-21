// Write-policy fixture for the conformance suite — the service-bound
// twin of policy.py: the same content-keyed verdicts over the strfry
// request line.
export default {
  async fetch(request) {
    const req = await request.json();
    const event = req.event || {};
    const content = event.content || "";
    let answer;
    if (content === "policy:reject") {
      answer = { id: event.id, action: "reject", msg: "blocked: test policy" };
    } else if (content === "policy:reject-empty") {
      answer = { id: event.id, action: "reject" };
    } else if (content === "policy:shadow") {
      answer = { id: event.id, action: "shadowReject", msg: "shadowed" };
    } else {
      answer = { id: event.id, action: "accept" };
    }
    return Response.json(answer);
  },
};
