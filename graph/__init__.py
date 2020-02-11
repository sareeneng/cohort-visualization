import logging
import itertools
import utilities as u


class Graph():
    def __init__(self):
        self.nodes = {}

    def add_node(self, name):
        if name in self.nodes:
            logging.error(f'{name} already in nodes')
            return
        self.nodes[name] = Node(name)

    def add_edge(self, start_node, end_node, start_label=None, end_label=None):
        # start_node and end_node are strings
        if start_node not in self.nodes:
            logging.error(f'{start_node} not in nodes.')
            return
        if end_node not in self.nodes:
            logging.error(f'{end_node} not in nodes.')
            return
        self.nodes[start_node].add_edge(other_node=end_node, this_label=start_label, other_label=end_label)

    def get_node_parents(self, node):
        # node has a parent when the other node has an edge pointing to this node, but this node does not have an edge pointing to the other node
        accessible_nodes = self.nodes[node].accessible_nodes
        parent_nodes = []
        for check_node in self.nodes:
            if check_node != node and check_node not in accessible_nodes:
                if node in self.nodes[check_node].accessible_nodes:
                    parent_nodes.append(check_node)
        
        return parent_nodes
    
    def get_node_children(self, node):
        # node has a child when this node has an edge pointing to the other node, but the other node does not have an edge pointing to this node
        accessible_nodes = self.nodes[node].accessible_nodes
        child_nodes = []
        for check_node in accessible_nodes:
            if node not in self.nodes[check_node].accessible_nodes:
                child_nodes.append(check_node)

        return child_nodes

    def get_node_siblings(self, node):
        # node has a sibling when this node has an edge pointing to the other node, and the other node has an edge pointing to this node
        accessible_nodes = self.nodes[node].accessible_nodes
        sibling_nodes = []
        for check_node in accessible_nodes:
            if node in self.nodes[check_node].accessible_nodes:
                sibling_nodes.append(check_node)

        return sibling_nodes

    def find_paths_between_nodes(self, start_node, end_node, current_path=[], search_depth=5):
        if start_node == end_node:
            return [start_node]
        
        all_paths = []

        current_path = current_path.copy() + [start_node]
        accessible_nodes = self.nodes[start_node].accessible_nodes

        if len(accessible_nodes) == 0:
            # end_node wasn't found in this path and there is nowhere else to go
            return []

        if end_node in accessible_nodes:
            # immediately return valid path rather than going through children/siblings
            all_paths.append(current_path.copy() + [end_node])
            return all_paths

        parent_nodes = self.get_node_parents(start_node)
        sibling_nodes = self.get_node_siblings(start_node)
        child_nodes = self.get_node_children(start_node)

        if end_node in parent_nodes:
            # if found in parents, then if parent is not a sibling of any of this nodes siblings, then there's no path to get up to that parent
            # if it is a sibling of a sibling, then return that sibling + destination node appended to current_path
            found_sibling_of_sibling = False
            for start_node_sibling in sibling_nodes:
                sibling_siblings = self.get_node_siblings(start_node_sibling)
                if end_node in sibling_siblings:
                    found_sibling_of_sibling = True
                    all_paths.append(current_path.copy() + [start_node_sibling] + [end_node])
            if found_sibling_of_sibling:
                return all_paths
            else:
                return []

        if len(current_path) >= search_depth:
            return []
        
        for child_node in child_nodes:
            for path in self.find_paths_between_nodes(start_node=child_node, end_node=end_node, current_path=current_path, search_depth=search_depth):
                all_paths.append(path)

        for sibling_node in sibling_nodes:
            if sibling_node not in current_path:  # prevents just looping back and forth between siblings forever
                for path in self.find_paths_between_nodes(start_node=sibling_node, end_node=end_node, current_path=current_path, search_depth=search_depth):
                    all_paths.append(path)
        
        return all_paths

    def find_paths_multi_nodes(self, list_of_nodes, fix_first=False, back_tracking_allowed=False):
        '''
        Given a list of nodes in any order, find a path that traverses all of them.

        If fix_first is True, then only look at paths which keep the first element the same
        '''

        if len(list_of_nodes) == 1:
            return [list_of_nodes]

        # first get all combos, these are candidate incomplete paths (missing intermediary tables)
        permutations = itertools.permutations(list_of_nodes)
        if fix_first:
            permutations = [x for x in permutations if x[0] == list_of_nodes[0]]

        valid_incomplete_paths = []
        for permutation in permutations:
            is_valid = True
            for pair in u.pairwise(permutation):
                if len(self.find_paths_between_nodes(pair[0], pair[1])) == 0:
                    is_valid = False
            if is_valid:
                valid_incomplete_paths.append(permutation)

        unflattened_valid_complete_paths = []
        for valid_incomplete_path in valid_incomplete_paths:
            path_possibilities_pairwise = []
            for pair in u.pairwise(valid_incomplete_path):
                path_possibilities_pairwise.append(self.find_paths_between_nodes(pair[0], pair[1]))
            combos = itertools.product(*path_possibilities_pairwise)
            for combo in combos:
                unflattened_valid_complete_paths.append(list(combo))
        
        flattened_valid_complete_paths = [list(u.flatten(l)) for l in unflattened_valid_complete_paths]

        flattened_valid_complete_paths = u.remove_adjacent_repeats(flattened_valid_complete_paths)

        if back_tracking_allowed is False:
            # remove paths that traverse a node twice
            flattened_valid_complete_paths = [x for x in flattened_valid_complete_paths if len(x) == len(set(x))]

        return flattened_valid_complete_paths


class Node():
    def __init__(self, name):
        self.name = name
        self.accessible_node_edges = {}

    @property
    def accessible_nodes(self):
        return self.accessible_node_edges.keys()

    def add_edge(self, other_node, this_label=None, other_label=None):
        # other_node is a string
        # can only have one edge per node
        if other_node in self.accessible_nodes:
            logging.error(f'{self.name} is already linked to {other_node}. Cannot add an extra edge with this directionality between the two.')
            return
        self.accessible_node_edges[other_node] = Edge(start_node=self, end_node=other_node, start_label=this_label, end_label=other_label)
    
    def __repr__(self):
        return f'Node {self.name}'


class Edge():
    def __init__(self, start_node, end_node, start_label=None, end_label=None):
        self.start_node = start_node
        self.end_node = end_node
        # user might provide only start_label or end_label, in that case assume the start_label and end_label are the same
        self.start_label = end_label if start_label is None else start_label
        self.end_label = start_label if end_label is None else end_label
