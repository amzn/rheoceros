# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Provide building blocks for future disassembly feature for applications.
 Instructions don't have any direct impact on how Applications are activated
 or how they are run in runtime. A pure dev-time concept.

- Instruction
- InstructionChain
"""

from typing import Any, Dict, List, NewType, Optional, Union

# from .node.base import Node
from intelliflow.core.entity import CoreData

from ...signal_processing import Signal
from ..core_application import ApplicationID


class InstructionLink(CoreData):
    def __init__(self, signal: "Signal", instruction: Optional["Instruction"] = None):
        self.signal = signal
        self.instruction = instruction
        # signifies that the link has been reset, it will be used to determine whether dest inst should be updated
        self.to_be_updated = False

    def is_equivalent(self, other: Union["Node", Signal]) -> bool:
        """Checks whether this link originates from or destined to same resource by using the signal equality without
        alias. Multiple links can originate from the same signal with different alias. So this is not an equals check.
        This method is the basis of detecting links between elements of an InstructionChain.
        """
        other_signal = other if isinstance(other, Signal) else other.signal()
        return self.signal.clone(alias=None, deep=False) == other_signal.clone(alias=None, deep=False)


class Instruction(CoreData):
    """Immutable class to hold high-level (user-level) instructions

    Actual actions/API calls against RheocerOS are serialized and persisted using
    this encapsulation. They are later on used to auto-generate application code
    in a different environment that does not have the actual code activated the
    application.

    Also Instruction class holds the high-level metadata required to see the application
    flow, in the form of links from external signals and between Instructions themselves.
    """

    def __init__(
        self,
        entity: Union["Node", ApplicationID],
        inputs: List["Signal"],
        user_command: str,
        args: List[Any],
        kwargs: Dict[str, Any],
        output_node: "Node",
        # interpreted version of this instruction (output_node is part of this DAG as well)
        symbol_tree: "Node",
    ) -> None:
        self.entity = entity
        self.user_command = user_command
        self.args = args
        self.kwargs = kwargs
        self.output_node = output_node
        self.symbol_tree = symbol_tree

        self.inbound: List[InstructionLink] = [InstructionLink(s) for s in inputs] if inputs else []
        # Child instruction::output_node.node_id vs links
        self.outbound: Dict[str, List[InstructionLink]] = dict()

    def resolve_links(self, parent: "Instruction") -> None:
        parent_signal = parent.output_node.signal()
        for link in self.inbound:
            # sanity check for 'link', to see if it is from within the same context
            if link.signal.resource_access_spec.get_owner_context_uuid() == parent_signal.resource_access_spec.get_owner_context_uuid():
                # neutralize alias' so that simple equality check would succeed
                if link.is_equivalent(parent_signal):
                    if link.to_be_updated:
                        if link.instruction.output_node.signal().check_integrity(parent_signal):
                            # we can remove this condition if new parent is still compatible with its prev version
                            # otherwise this child should be updated (and replaced totally) to reset this flag.
                            link.to_be_updated = False
                            # update
                            link.instruction = parent
                    else:
                        link.instruction = parent

                    # keep iterating as multiple links might be from the same parent (same signal with different alias')
                    parent.outbound.setdefault(self.output_node.node_id, []).append(InstructionLink(link.signal, self))

    def remove_links(self) -> None:
        # remove upstream connections
        for link in self.inbound:
            parent = link.instruction
            if parent:
                if self.output_node.node_id in parent.outbound:  # multiple links might have the same parent (delete once)
                    del parent.outbound[self.output_node.node_id]
                link.instruction = None

        # remove downstream connections
        for down_links in self.outbound.values():
            # get downstream instruction reference from the first one (there might be multiple links into same node)
            down_link = down_links[0]
            # reset upstream links of this downstream instruction
            for link in down_link.instruction.inbound:
                if link.is_equivalent(self.output_node):
                    link.to_be_updated = True

    def get_inbound_links(self, parent: Union[Signal, "Instruction"]) -> List[InstructionLink]:
        parent_signal = parent.output_node.signal() if isinstance(parent, Instruction) else parent
        links = []
        for link in self.inbound:
            if link.signal.resource_access_spec.get_owner_context_uuid() == parent_signal.resource_access_spec.get_owner_context_uuid():
                if link.is_equivalent(parent_signal):
                    links.append(link)
        return links

    def is_dirty(self) -> bool:
        """Check whether one of the inbound links has been updated in a bad way, leaving this orphaned or incompatible
        Cases:
        - Internal Error: An intermediate node has been removed (probably during a node update [Application::update_data])
        and not successfully restored (injected back) into the instruction chain. Or it was injected but downstream
        instructions' inbound references not set as expected (this is when 'link.updated == True').
        """
        return any(link.to_be_updated for link in self.inbound)

    def get_dependency_set(self) -> List["Instruction"]:
        children: List[Instruction] = []
        for down_links in self.outbound.values():
            down_link = down_links[0]
            children.append(down_link.instruction)
            children.extend(down_link.instruction.get_dependency_set())
        return children


InstructionChain = NewType("InstructionChain", List[Instruction])
