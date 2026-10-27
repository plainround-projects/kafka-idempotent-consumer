# JSON

## What it is

JSON (JavaScript Object Notation) is a plain-text format for structured data. A JSON value is a number, text in double quotes, `true`, `false`, `null`, a list in square brackets, or an object in curly braces made of `"name": value` pairs. Almost every language reads and writes it.

## Why this repository uses it

Payments cross the network as bytes in Kafka records. The lesson uses JSON events. JSON is also used for two files the repository writes or reads: the dead-letter record and `expected_totals.json`.

## Where it appears

| Path | What it does with JSON |
| --- | --- |
| Kafka topic `payments` | Each record's value is one JSON object `{"eventId": ..., "accountId": ..., "amount": ...}`. |
| `src/idempotent_consumer/domain/payments.py` | `parse_payment` reads and checks it; `Payment.payload` builds it. |
| `src/idempotent_consumer/domain/dead_letter.py`, `kafka_producers.py` | The dead-letter record is JSON. |
| `src/idempotent_consumer/infrastructure/expected_file.py` | Writes and reads `expected_totals.json`. |

## How it works underneath

`json.dumps(object)` produces text; `.encode()` turns it into bytes (UTF-8). `json.loads(bytes)` reads bytes back into Python dictionaries, lists, text and numbers. JSON does not say which fields are required or what a number means. Also, JSON has one kind of number: this code sends amounts as numbers with two decimals (`120.5`) and reads them through `Decimal(str(value))` so no precision is lost.

## Walkthrough of this repository's use

```python
def parse_payment(raw: bytes) -> Payment:
    """Validate one record's value. Raises PoisonMessageError with the reason when it is unusable."""
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as error:
        raise PoisonMessageError(f"not valid JSON: {error}") from error
    if not isinstance(data, dict):
        raise PoisonMessageError("payload is not a JSON object")
    try:
        event_id = str(uuid.UUID(str(data["eventId"])))
    except (KeyError, ValueError) as error:
        raise PoisonMessageError(f"eventId missing or not a uuid: {data.get('eventId')!r}") from error
    account_id = data.get("accountId")
    if not isinstance(account_id, str) or not account_id:
        raise PoisonMessageError(f"accountId missing or not text: {account_id!r}")
    return Payment(event_id, account_id, parse_amount(data.get("amount")))
```

- `json.loads(raw)` can fail for text that is not JSON (`ValueError`) or bytes that are not text (`UnicodeDecodeError`); both become `PoisonMessageError("not valid JSON: ...")`.
- The result must be an object (`dict`), not a list or number.
- `str(uuid.UUID(str(data["eventId"])))` checks that the id is a real UUID; a missing key raises `KeyError`, also turned into poison.
- `accountId` must be non-empty text. The amount goes through `parse_amount` (`tech/python.md`).

Anything that fails is **poison**: it can never be processed, however often it is retried, so it is parked in the dead-letter topic.

### The dead-letter record

```python
    def payload(self) -> dict[str, Any]:
        return {
            "originalTopic": self.original_topic,
            "originalPartition": self.original_partition,
            "originalOffset": self.original_offset,
            "error": self.error,
            "raw": self.raw.decode("utf-8", errors="replace"),
            # The exact bytes, so a replay is byte-faithful even when they were not valid text.
            "rawBase64": base64.b64encode(self.raw).decode("ascii"),
        }
```

A dead letter keeps where the record came from (`originalTopic`, `originalPartition`, `originalOffset`), why it failed (`error`), the readable text (`raw`) and the exact bytes in Base64 (`rawBase64`), so it can be replayed byte for byte even if the original was not valid text.

### The expected totals file

```python
def save_expected(path: Path, totals: Mapping[str, Decimal]) -> None:
    """Write the expected totals as JSON."""
    path.write_text(json.dumps({account: str(total) for account, total in totals.items()}, indent=1))


def load_expected(path: Path) -> dict[str, Decimal]:
    """Read the expected totals written by `save_expected`."""
    return {account: Decimal(total) for account, total in json.loads(path.read_text()).items()}
```

Amounts are stored as strings (`"4821.33"`) so no number type can round them.

## Try it yourself

1. `.venv/bin/python -c "import json; print(json.loads(b'{\"a\": 1.5}'))"` prints `{'a': 1.5}`.
2. `.venv/bin/python -c "from idempotent_consumer.domain.payments import parse_payment; parse_payment(b'{broken')"` ends with `PoisonMessageError: not valid JSON: ...`.
3. After `make demo-poison`, the dead-letter record printed by the console consumer is one JSON object with `originalTopic`, `error`, `raw` and `rawBase64`.
4. `head -c 200 expected_totals.json` (after `make data`) shows `{ "acct-001": "54617.11", ...`.

## Mistakes people make

- Trusting parsed JSON without checking each field.
- Storing money as a JSON number and reading it as a float.
- Printing bytes and text as if they were the same thing.

## Check yourself

1. What does `json.loads` need as input? *Text or bytes containing JSON.*
2. What happens to a record that is not JSON? *It raises `PoisonMessageError` and goes to the dead-letter topic.*
3. Why is `rawBase64` stored too? *To keep the exact bytes for replay.*
4. Why store totals as strings in the file? *So no number type rounds them.*
5. Why check that the payload is an object? *A list or number is valid JSON but not a payment.*

## Further reading

- JSON (ECMA-404): https://ecma-international.org/publications-and-standards/standards/ecma-404/
- Python 3.12 `json`: https://docs.python.org/3.12/library/json.html
- Python 3.12 `base64`: https://docs.python.org/3.12/library/base64.html
