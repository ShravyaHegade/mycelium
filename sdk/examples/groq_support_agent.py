"""A customer support agent that uses Groq and protects refunds with Mycelium."""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from mycelium import load_config_from_string, side_effect

DEFAULT_MODEL = "openai/gpt-oss-20b"
DEFAULT_GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

Decision = Mapping[str, Any]
DecisionModel = Callable[[str, str, Mapping[str, Decimal]], Decision]
RefundProvider = Callable[[str, Decimal, str], Mapping[str, Any]]


class AgentError(RuntimeError):
    """The agent could not safely decide or execute the request."""


class GroqDecisionModel:
    """OpenAI-compatible Groq decision call using only the standard library."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        model: str | None = None,
        url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.api_key = api_key or os.environ.get("GROQ_API_KEY", "")
        if not self.api_key:
            raise AgentError("GROQ_API_KEY is required")
        self.model = model or os.environ.get("GROQ_MODEL", DEFAULT_MODEL)
        self.url = url or os.environ.get("GROQ_CHAT_URL", DEFAULT_GROQ_URL)
        self.timeout = timeout

    def __call__(
        self,
        ticket_id: str,
        customer_message: str,
        orders: Mapping[str, Decimal],
    ) -> Decision:
        order_context = {order_id: str(amount) for order_id, amount in orders.items()}
        payload = json.dumps(
            {
                "model": self.model,
                "temperature": 0,
                "response_format": {"type": "json_object"},
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "You are a customer support agent. Return JSON only. Choose either "
                            '{"action":"reply","message":"..."} or '
                            '{"action":"refund","order_id":"...","amount":"0.00",'
                            '"message":"..."}. Refund only an order shown in the supplied order '
                            "data and never refund more than its paid amount."
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps(
                            {
                                "ticket_id": ticket_id,
                                "customer_message": customer_message,
                                "orders": order_context,
                            }
                        ),
                    },
                ],
            }
        ).encode("utf-8")
        request = Request(
            self.url,
            data=payload,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:  # noqa: S310
                body = json.loads(response.read())
        except HTTPError as exc:
            raise AgentError(f"Groq returned HTTP {exc.code}") from exc
        except (OSError, URLError, json.JSONDecodeError) as exc:
            raise AgentError("Groq request failed") from exc

        try:
            content = body["choices"][0]["message"]["content"]
            decision = json.loads(content)
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            raise AgentError("Groq returned an invalid decision") from exc
        if not isinstance(decision, dict):
            raise AgentError("Groq decision must be a JSON object")
        return decision


def _mycelium_config(ledger_path: Path) -> str:
    return f"""
transition:
  agent_id: customer-support-agent
  policy_version: "1"

action_ledger:
  storage: sqlite
  path: {json.dumps(str(ledger_path))}
  request_identity_policy: require_explicit
  tools: [issue_refund]

tools:
  issue_refund:
    side_effect_class: keyed_mutate
    provider_idempotency_key_param: idempotency_key
"""


class CustomerSupportAgent:
    """A real agent application written by a consumer of Mycelium."""

    def __init__(
        self,
        refund_provider: RefundProvider,
        *,
        state_dir: str | Path,
        decision_model: DecisionModel | None = None,
    ) -> None:
        self.refund_provider = refund_provider
        self.decision_model = (
            decision_model if decision_model is not None else GroqDecisionModel()
        )
        state_path = Path(state_dir).expanduser().resolve()
        state_path.mkdir(parents=True, exist_ok=True)
        self.config = load_config_from_string(_mycelium_config(state_path / "ledger.db"))

        @self.config.apply
        def issue_refund(
            order_id: str,
            amount: str,
            idempotency_key: str,
        ) -> Mapping[str, Any]:
            with side_effect():
                return self.refund_provider(
                    order_id,
                    Decimal(amount),
                    idempotency_key,
                )

        self.issue_refund = issue_refund

    def handle(
        self,
        *,
        ticket_id: str,
        customer_message: str,
        orders: Mapping[str, Decimal | str | int | float],
    ) -> dict[str, Any]:
        """Decide a support ticket and execute at most one refund per ticket/order."""
        if not ticket_id.strip():
            raise AgentError("ticket_id must be non-empty")
        known_orders = self._normalize_orders(orders)
        decision = self.decision_model(ticket_id, customer_message, known_orders)
        action = decision.get("action")
        if action == "reply":
            return {
                "action": "reply",
                "message": str(decision.get("message", "")),
            }
        if action != "refund":
            raise AgentError("model selected an unsupported action")

        order_id = str(decision.get("order_id", ""))
        if order_id not in known_orders:
            raise AgentError("model selected an unknown order")
        try:
            amount = Decimal(str(decision["amount"]))
        except (KeyError, InvalidOperation) as exc:
            raise AgentError("model selected an invalid refund amount") from exc
        if not amount.is_finite() or amount <= 0 or amount > known_orders[order_id]:
            raise AgentError("refund amount is outside the paid amount")

        amount_text = str(amount.quantize(Decimal("0.01")))
        request_id = f"support-refund:{ticket_id}:{order_id}"
        with self.config.run(f"support-ticket:{ticket_id}"):
            refund = self.issue_refund(
                order_id=order_id,
                amount=amount_text,
                idempotency_key=request_id,
                request_id=request_id,
            )
        return {
            "action": "refund",
            "message": str(decision.get("message", "")),
            "refund": dict(refund),
        }

    @staticmethod
    def _normalize_orders(
        orders: Mapping[str, Decimal | str | int | float],
    ) -> dict[str, Decimal]:
        normalized: dict[str, Decimal] = {}
        for order_id, paid in orders.items():
            try:
                amount = Decimal(str(paid))
            except InvalidOperation as exc:
                raise AgentError(f"invalid paid amount for order {order_id!r}") from exc
            if not order_id or not amount.is_finite() or amount < 0:
                raise AgentError("order IDs must be non-empty and paid amounts non-negative")
            normalized[str(order_id)] = amount
        return normalized
