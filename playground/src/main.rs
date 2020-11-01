// ############################################################################
// Playground
// ############################################################################
use petgraph;

fn main() {
    let mut myg: petgraph::Graph<&str, (), petgraph::Directed> = petgraph::Graph::new();
    let node1 = myg.add_node("node1");
    let node2 = myg.add_node("node2");
    let node3 = myg.add_node("node3");
    myg.add_edge(node1, node2, ());
    myg.add_edge(node1, node3, ());

    println!("Node 1 {:?}", node1);
    for neighbor in myg.neighbors_directed(node1, petgraph::Direction::Outgoing) {
        println!("{:?}", myg[neighbor]);
    }
}
