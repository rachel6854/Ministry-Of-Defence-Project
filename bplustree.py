"""Simple implementation of a B+ tree, a self-balancing tree data structure that (1) maintains sort
data order and (2) allows insertions and access in logarithmic time.
"""


class Node(object):
    """Base node object.
    Each node stores keys and values. Keys are not unique to each value, and as such values are
    stored as a list under each key.
    Attributes:
        order (int): The maximum number of keys each node can hold.
    """


def create_node(order):
    """Child nodes can be converted into parent nodes by setting node["leaf = False. Parent nodes
    simply act as a medium to traverse the tree."""
    return {
        "order": order,
        "keys": [],
        "values": [],
        "leaf": True
    }


def add(node, key, value):
    """Adds a key-value pair to the node."""
    # If the node is empty, simply insert the key-value pair.
    if not node["keys"]:
        node["keys"].append(key)
        node["values"].append([value])
        return None

    for i, item in enumerate(node["keys"]):
        # If new key matches existing key, add to list of values.
        if key == item:
            node["values"][i].append(value)
            break

        # If new key is smaller than existing key, insert new key to the left of existing key.
        elif key < item:
            node["keys"] = node["keys"][:i] + [key] + node["keys"][i:]
            node["values"] = node["values"][:i] + [[value]] + node["values"][i:]
            break

        # If new key is larger than all existing keys, insert new key to the right of all
        # existing keys.
        elif i + 1 == len(node["keys"]):
            node["keys"].append(key)
            node["values"].append([value])


def split(node):
    """Splits the node into two and stores them as child nodes."""
    left = create_node(node["order"])
    right = create_node(node["order"])
    mid = node["order"] // 2

    left["keys"] = node["keys"][:mid]
    left["values"] = node["values"][:mid]

    right["keys"] = node["keys"][mid:]
    right["values"] = node["values"][mid:]

    # When the node is split, set the parent key to the left-most key of the right child node.
    node["keys"] = [right["keys"][0]]
    node["values"] = [left, right]
    node["leaf"] = False


def is_full(node):
    """Returns True if the node is full."""
    return len(node["keys"]) == node["order"]


def show_node(node, counter=0):
    """Prints the keys at each level."""
    print(counter, str(node["keys"]))

    # Recursively print the key of child nodes (if these exist).
    if not node["leaf"]:
        for item in node["values"]:
            show_node(item, counter + 1)


"""B+ tree object, consisting of nodes.
Nodes will automatically be split into two once it is full. When a split occurs, a key will
'float' upwards and be inserted into the parent node to act as a pivot.
Attributes:
    order (int): The maximum number of keys each node can hold.
"""


def create_bplustree(order=8):
    return {
        "root": create_node(order)
    }


def _find(node, key):
    """ For a given node and key, returns the index where the key should be inserted and the
    list of values at that index."""
    for i, item in enumerate(node["keys"]):
        if key < item:
            return node["values"][i], i

    return node["values"][i + 1], i + 1


def _merge(parent, child, index):
    """For a parent and child node, extract a pivot from the child to be inserted into the keys
    of the parent. Insert the values from the child into the values of the parent.
    """
    parent["values"].pop(index)
    pivot = child["keys"][0]

    for i, item in enumerate(parent["keys"]):
        if pivot < item:
            parent["keys"] = parent["keys"][:i] + [pivot] + parent["keys"][i:]
            parent["values"] = parent["values"][:i] + child["values"] + parent["values"][i:]
            break

        elif i + 1 == len(parent["keys"]):
            parent["keys"] += [pivot]
            parent["values"] += child["values"]
            break


def insert(bplustree, key, value):
    """Inserts a key-value pair after traversing to a leaf node. If the leaf node is full, split
    the leaf node into two.
    """
    parent = None
    child = bplustree["root"]

    # Traverse tree until leaf node is reached.
    while not child["leaf"]:
        parent = child
        child, index = _find(child, key)

    add(child, key, value)

    # If the leaf node is full, split the leaf node into two.
    if is_full(child):
        split(child)

        # Once a leaf node is split, it consists of a internal node and two leaf nodes. These
        # need to be re-inserted back into the tree.
        if parent and not is_full(parent):
            _merge(parent, child, index)


def retrieve(bplustree, key):
    """Returns a value for a given key, and None if the key does not exist."""
    child = bplustree["root"]

    while not child["leaf"]:
        child, index = _find(child, key)

    for i, item in enumerate(child["keys"]):
        if key == item:
            return child["values"][i]

    return None


def update(bplustree, key, new_value):
    """Returns a value for a given key, and None if the key does not exist."""
    child = bplustree["root"]

    while not child["leaf"]:
        child, index = _find(child, key)

    for i, item in enumerate(child["keys"]):
        if key == item:
            child["values"][i] = new_value
            return True  # success

    return False  # faild


def delete(bplustree, key):
    update(bplustree, key, None)


def show_bplustree(bplustree):
    """Prints the keys at each level."""
    show_node(bplustree["root"])


def demo_node():
    print('Initializing node...')
    node = create_node(order=4)

    print('\nInserting key a...')
    add(node, 'a', 'alpha')
    print('Is node full?', is_full(node))
    show_node(node)

    print('\nInserting keys b, c, d...')
    add(node, 'b', 'bravo')
    add(node, 'c', 'charlie')
    add(node, 'd', 'delta')
    print('Is node full?', is_full(node))
    show_node(node)

    print('\nSplitting node...')
    split(node)
    show_node(node)


def demo_bplustree():
    print('Initializing B+ tree...')
    bplustree = create_bplustree(order=4)

    print('\nB+ tree with 1 item...')
    insert(bplustree, 'a', 'alpha')
    show_bplustree(bplustree)

    insert(bplustree, 'a', 'akjjkh')
    show_bplustree(bplustree)

    print('\nB+ tree with 2 items...')
    insert(bplustree, 'b', 'bravo')
    show_bplustree(bplustree)

    print('\nB+ tree with 3 items...')
    insert(bplustree, 'c', 'charlie')
    show_bplustree(bplustree)

    print('\nB+ tree with 4 items...')
    insert(bplustree, 'd', 'delta')
    show_bplustree(bplustree)

    print('\nB+ tree with 5 items...')
    insert(bplustree, 'e', 'echo')
    show_bplustree(bplustree)

    print('\nB+ tree with 6 items...')
    insert(bplustree, 'f', 'foxtrot')
    show_bplustree(bplustree)

    print('\nRetrieving values with key e...')
    print(retrieve(bplustree, 'e'))
    print(bplustree)
    print(retrieve(bplustree, 'a'))


if __name__ == '__main__':
    demo_node()
    print('\n')
    demo_bplustree()
