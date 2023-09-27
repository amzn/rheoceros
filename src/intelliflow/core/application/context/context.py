# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Module to encapsulate Application code in RheocerOS' internal entity model.

   - Context:

"""
import copy
import logging
from typing import Any, Dict, List, NewType, Optional, Set, Type

from intelliflow.core.application.context.traversal import ContextVisitor
from intelliflow.core.platform.constructs import BaseConstruct, ConstructSecurityConf
from intelliflow.core.serialization import Serializable, dumps, loads

from ...platform.development import Configuration, HostPlatform
from ...platform.platform import Platform
from ..remote_application import DownstreamApplication, RemoteApplication
from .instruction import Instruction, InstructionChain
from .node.base import Node

logger = logging.getLogger(__name__)

_SymbolTable = NewType("_SymbolTable", Dict[str, Node])
_ImportedData = NewType("_ImportedData", Set[RemoteApplication])
_DownstreamApps = NewType("_DownstreamApps", Set[DownstreamApplication])
_SecurityConf = NewType("_SecurityConf", Dict[Type[BaseConstruct], ConstructSecurityConf])
_Dashboards = NewType("_Dashboards", Dict[str, Any])


class Context(Serializable["Context"]):
    """Class to hold interpreted user code in the form of Nodes (as the underlying symbol tree).
    Additionally, it keeps a track of high-level instructions for disassembling user-code in
    different environments. This secondary functionality has no direct runtime effect.

    From Application module's perspective, acts as a container for user <Instruction>s and
    more importantly for the node-system which is the low-level breakdown of each Instruction.
    """

    def __init__(self, other: Optional["Context"] = None) -> None:
        if other is None:
            self._instruction_chain = InstructionChain([])
            self._internal_data = _SymbolTable(dict())

            self._imported_data = _ImportedData(set())
            self._downstream_apps = _DownstreamApps(set())

            self._security_conf = _SecurityConf(dict())

            self._dashboards = _Dashboards(dict())
        else:
            # make sure cloned instruction chain will be decoupled from other's topological changes
            self._instruction_chain = InstructionChain([copy.deepcopy(inst) for inst in other.instruction_chain])
            self._internal_data = _SymbolTable(dict(other.internal_data))

            self._imported_data = _ImportedData(set(other.external_data))
            self._downstream_apps = _DownstreamApps(set(other.downstream_dependencies))

            self._security_conf = _SecurityConf(dict(other.security_conf))

            # TODO remove custom dashboards release (temporary backwards compatibility)
            if getattr(other, "_dashboards", None):
                self._dashboards = _Dashboards(dict(other.dashboards))
            else:
                self._dashboards = _Dashboards(dict())

    def _serializable_copy_init(self, org_instance: "Context") -> None:
        super()._serializable_copy_init(org_instance)
        # instruction_chain shallow copy should be serializable already
        # self._instruction_chain = self._instruction_chain.serializable_copy()

        self._imported_data = {imported_app.serializable_copy() for imported_app in self._imported_data}

    def _deserialized_init(self, platform: Platform) -> None:
        if isinstance(platform, HostPlatform):
            for imported_app in self._imported_data:
                imported_app._deserialized_init(platform)
        else:
            logger.info(
                f"Skipping deserialization/load operation for the transitive dependencies of;"
                f"{platform.__class__.__name__}, context_id={platform.context_id}, conf={platform.conf.__class__}"
            )

    @property
    def instruction_chain(self) -> InstructionChain:
        return self._instruction_chain

    @property
    def internal_data(self) -> _SymbolTable:
        return self._internal_data

    @property
    def external_data(self) -> Set[RemoteApplication]:
        return self._imported_data

    @property
    def downstream_dependencies(self) -> Set[DownstreamApplication]:
        return self._downstream_apps

    @property
    def security_conf(self) -> _SecurityConf:
        return self._security_conf

    @property
    def dashboards(self) -> _Dashboards:
        return self._dashboards

    def add_instruction(self, instruction: Instruction) -> None:
        self.insert_instruction(len(self._instruction_chain), instruction)

    def get_instruction_index(self, output_node: "Node") -> int:
        for i, instruction in enumerate(self._instruction_chain):
            if instruction.output_node == output_node:
                return i

        raise ValueError(f"Instruction cannot be found for output_node: {output_node!r}")

    def get_instructions(self, output_node: "Node") -> List[Instruction]:
        return [inst for inst in self._instruction_chain if inst.output_node == output_node]

    def get_dependency_set(self, output_node: "Node") -> List[Instruction]:
        instructions = self.get_instructions(output_node)
        latest_inst: Instruction = instructions[-1:][0]
        return latest_inst.get_dependency_set()

    def remove_instruction(self, output_node: "Node") -> int:
        removed_index: int = None
        for i, instruction in enumerate(self._instruction_chain):
            if instruction.output_node == output_node:
                removed_index = i
                instruction.remove_links()
                self._instruction_chain.pop(i)
                break

        if removed_index is not None:
            del self._internal_data[output_node.node_id]
        else:
            raise ValueError(f"Instruction cannot be found for output_node: {output_node!r}")

        return removed_index

    def check_referential_integrity(self) -> None:
        """Raises if any of the instructions is detected as orphaned, meaning that its inbound links are stale.
        Cases:
         - Internal error for framework's application level logic to manage instruction graph following API calls
        In case of an integrity, this method is expected to raise
         - User explicitly accesses application context and instruction graph and messes up. This is still a best-effort
         sanity-check to prevent further frustration in debugging a runtime system that would be in an undefined state.
        """
        for inst in self._instruction_chain:
            if inst.is_dirty():
                raise TypeError(
                    f"Following instruction is in dangling/stale state: {inst.output_node.signal().alias!r}!"
                    f" It needs to be updated with new input references."
                )

    def insert_instruction(self, index: int, instruction: Instruction) -> None:
        # scan preceding instructions as possible parents
        for parent_inst in self._instruction_chain[:index]:
            instruction.resolve_links(parent=parent_inst)
        # scan succeeding instructions as possible dependents
        for child_ins in self._instruction_chain[index:]:
            child_ins.resolve_links(parent=instruction)
        self._instruction_chain.insert(index, instruction)
        self._internal_data[instruction.output_node.node_id] = instruction.output_node

    def add_upstream_app(self, remote_app: RemoteApplication) -> None:
        self._imported_data.add(remote_app)

    def add_downstream_app(self, id: str, conf: Configuration):
        self._downstream_apps.add(DownstreamApplication(id, conf))

    def has_upstream_app(self) -> bool:
        return bool(self._imported_data)

    def get_upstream_app(self, context_uuid: str) -> RemoteApplication:
        for remote_app in self._imported_data:
            if remote_app.uuid == context_uuid:
                return remote_app
        return None

    def add_security_conf(self, construct: Type[BaseConstruct], conf: ConstructSecurityConf):
        self._security_conf[construct] = conf

    def add_dashboard(self, dashboard_id: str, data: Dict[str, Any]):
        self._dashboards[dashboard_id] = data

    def get_dashboard(self, dashboard_id: str) -> Optional[Dict[str, Any]]:
        return self._dashboards.get(dashboard_id, None)

    def clone(self) -> "Context":
        return Context(self)

    def activate(self, host_platform: HostPlatform) -> None:
        """Follow instruction chain in order and let root <Node>s to activate themselves

        This can be evaluated as the actual interpretation of the user-code and
        at the end of this activation cycle it is expected to have the platform
        side effect (signal-slot) of each <Node> to be registered (against the <RoutingTable>
        for example). But, of course, Node impls are free to take other measures and do
        operations as part of this cycle.

        Parameters
        ----------
        host_platform: HostPlatform
            Development time platform instance which will provide the necessary Constructs
            to Nodes so that they dump their connection (Signal, Slot) records.
        """
        for instruction in self._instruction_chain:
            instruction.symbol_tree.activate(host_platform)

        for imported_app in self._imported_data:
            host_platform.connect_upstream(imported_app.platform)

        for downstream_app in self._downstream_apps:
            host_platform.connect_downstream(downstream_app.id, downstream_app.conf)

        host_platform.add_security_conf(self._security_conf)
        host_platform.add_dashboards(self._dashboards)

    def accept(self, visitor: ContextVisitor) -> None:
        for instruction in self._instruction_chain:
            instruction.symbol_tree.accept(visitor)


__test__ = {name: value for name, value in locals().items() if name.startswith("test_")}

if __name__ == "__main__":
    import doctest

    doctest.testmod(verbose=False)
