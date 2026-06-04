from itertools import pairwise

import pytest

from orchestrator.core.errors import InvalidTransitionError
from orchestrator.core.state_machine import NodeStateMachine
from orchestrator.core.statuses import NodeStatus


def test_happy_path_transitions():
    chain = [
        NodeStatus.PENDING,
        NodeStatus.RUNNABLE,
        NodeStatus.ENQUEUED,
        NodeStatus.DISPATCHED,
        NodeStatus.WAITING_RESULT,
        NodeStatus.COMPLETED,
    ]
    for current, target in pairwise(chain):
        assert NodeStateMachine.transition(current, target) == target


def test_dispatch_error_retry_loop():
    assert NodeStateMachine.can_transition(NodeStatus.DISPATCHED, NodeStatus.DISPATCH_ERROR)
    assert NodeStateMachine.can_transition(NodeStatus.DISPATCH_ERROR, NodeStatus.ENQUEUED)
    assert NodeStateMachine.can_transition(NodeStatus.DISPATCH_ERROR, NodeStatus.DISPATCH_FAILED)


def test_fire_and_forget_dispatched_to_completed():
    assert NodeStateMachine.can_transition(NodeStatus.DISPATCHED, NodeStatus.COMPLETED)


def test_terminal_has_no_transitions():
    with pytest.raises(InvalidTransitionError):
        NodeStateMachine.transition(NodeStatus.COMPLETED, NodeStatus.RUNNABLE)
    with pytest.raises(InvalidTransitionError):
        NodeStateMachine.transition(NodeStatus.DISPATCH_FAILED, NodeStatus.ENQUEUED)


def test_invalid_transition_raises():
    with pytest.raises(InvalidTransitionError):
        NodeStateMachine.transition(NodeStatus.PENDING, NodeStatus.COMPLETED)


def test_dispatch_error_is_not_terminal_transition_target_from_waiting():
    assert not NodeStateMachine.can_transition(NodeStatus.WAITING_RESULT, NodeStatus.DISPATCH_ERROR)
