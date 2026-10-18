# Test vectors

Vendored from the `qntx/nostr` repository (`nostr.js` workspace) at commit
`466a37c`. These files are **generated**; never edit them by hand. To
refresh, re-copy:

```bash
cp $NOSTR_JS/vectors/core/{event-validate,event-serialize,event-order,filter-match,filter-limit,kind-classify,message-client}.json crates/nkr/tests/vectors/
cp $NOSTR_JS/vectors/bip340/official.csv crates/nkr/tests/vectors/bip340/official.csv
```

Legitimate divergences between the reference TypeScript implementation
and this crate are recorded per-runner in `const DIVERGENCES` lists inside
`crates/nkr/tests/`:

- serde rejects duplicate keys where `JSON.parse` takes the last value;
- serde rejects float spellings (`1e3`, `1.0`, `-0`) that `JSON.parse`
  accepts;
- `nkr` follows strfry relay policy: unknown filter keys, `search`, and
  `#` names that are not exactly one ASCII letter are rejected with
  `unsupported:`, where the SDK canonicalizer drops them;
- empty `#` value lists make a filter impossible (NIP-01 MUST) where the
  SDK drops the condition;
- `since`/`until`/`limit` accept the full `u64` range where the SDK
  enforces `Number.isSafeInteger` (`2^53 - 1`).
