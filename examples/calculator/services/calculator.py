import ast
import operator
from typing import Any

from common import NATS_URL, RESULTS_SUBJECT, build_error, build_result
from faststream import FastStream
from faststream.nats import NatsBroker

broker = NatsBroker(NATS_URL)
app = FastStream(broker)

_BINARY = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_UNARY = {ast.UAdd: operator.pos, ast.USub: operator.neg}


def evaluate(expression: str) -> float:
    """Вычислить арифметическое выражение, разрешая только числа и операторы."""

    def visit(node: ast.AST) -> float:
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        if isinstance(node, ast.BinOp) and type(node.op) in _BINARY:
            return _BINARY[type(node.op)](visit(node.left), visit(node.right))
        if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY:
            return _UNARY[type(node.op)](visit(node.operand))
        raise ValueError(f"недопустимое выражение: {expression!r}")

    return visit(ast.parse(expression, mode="eval").body)


@broker.subscriber("service.calc")
async def calculate(envelope: dict[str, Any]) -> None:
    expression = envelope["data"]["expression"]
    try:
        value = evaluate(expression)
    except (ValueError, SyntaxError, ZeroDivisionError) as exc:
        await broker.publish(build_error(envelope, str(exc)), RESULTS_SUBJECT)
        return
    await broker.publish(build_result(envelope, {"value": value}), RESULTS_SUBJECT)


if __name__ == "__main__":
    import asyncio

    asyncio.run(app.run())
