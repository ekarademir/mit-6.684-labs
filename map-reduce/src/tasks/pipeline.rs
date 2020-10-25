use std::iter::IntoIterator;

use petgraph::{self, visit};

use super::task_assignment::ATask;

pub type TaskGraph = petgraph::Graph<ATask, (), petgraph::Directed>;

pub struct Pipeline {
  pub task_graph: TaskGraph,
}

impl IntoIterator for Pipeline {
  type Item = ATask;
  type IntoIter = std::vec::IntoIter<Self::Item>;

  fn into_iter(self) -> Self::IntoIter {
      let mut linear: Vec<ATask> = Vec::new();

      // TODO Create a linearizarion of graph, Breadth first visit

      linear.into_iter()
  }
}
