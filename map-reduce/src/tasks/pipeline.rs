use std::iter::IntoIterator;

use petgraph::{
  self,
  graph,
  visit,
};

use super::task_assignment::ATask;

type TaskGraph = petgraph::Graph<ATask, (), petgraph::Directed>;
type TaskNode = graph::NodeIndex;

pub struct Pipeline {
  inner: TaskGraph,
}

impl Pipeline {
  pub fn new() -> Self {
    let inner = TaskGraph::new();
    Pipeline {
      inner
    }
  }

  pub fn add_task(&mut self, task: ATask) -> TaskNode {
    self.inner.add_node(task)
  }

  pub fn add_order(&mut self, from: TaskNode, to: TaskNode) {
    self.inner.add_edge(from, to, ());
  }
}

impl IntoIterator for Pipeline {
  type Item = ATask;
  type IntoIter = std::vec::IntoIter<Self::Item>;

  fn into_iter(self) -> Self::IntoIter {
      let mut linear: Vec<ATask> = Vec::new();

      let mut topological_sort = visit::Topo::new(&self.inner);
      while let Some(node_idx) = topological_sort.next(&self.inner) {
        linear.push(self.inner[node_idx]);
      }

      linear.into_iter()
  }
}

#[cfg(test)]
mod tests {
  #[test]
  #[cfg_attr(feature = "dont_test_this", ignore)]
  fn test_linear_dag() {
    let mut task_pipeline = super::Pipeline::new();
    let count_words = task_pipeline.add_task(super::ATask::CountWords);
    let sum_counts = task_pipeline.add_task(super::ATask::SumCounts);

    task_pipeline.add_order(count_words, sum_counts);

    // count_words -> sum_counts


    let expected = vec![
      super::ATask::CountWords,
      super::ATask::SumCounts
    ];

    for (idx, task) in task_pipeline.into_iter().enumerate() {
      assert_eq!(expected[idx], task);
    }
  }

  #[test]
  #[cfg_attr(feature = "dont_test_this", ignore)]
  fn test_cyclic() {
    let mut task_pipeline = super::Pipeline::new();
    let count_words = task_pipeline.add_task(super::ATask::CountWords);
    let sum_counts = task_pipeline.add_task(super::ATask::SumCounts);

    task_pipeline.add_order(count_words, sum_counts);
    task_pipeline.add_order(sum_counts, count_words);

    // count_words -> sum_counts
    //            \__/

    let expected: Vec<super::ATask> = Vec::new();

    for (idx, task) in task_pipeline.into_iter().enumerate() {
      assert_eq!(expected[idx], task);
    }
  }

  #[test]
  #[cfg_attr(feature = "dont_test_this", ignore)]
  fn test_multi_level_dag() {
    let mut task_pipeline = super::Pipeline::new();
    let count_words1 = task_pipeline.add_task(super::ATask::CountWords);
    let sum_counts1 = task_pipeline.add_task(super::ATask::SumCounts);
    let count_words2 = task_pipeline.add_task(super::ATask::CountWords);
    let sum_counts2 = task_pipeline.add_task(super::ATask::SumCounts);

    task_pipeline.add_order(sum_counts1, sum_counts2);
    task_pipeline.add_order(count_words1, sum_counts2);
    task_pipeline.add_order(sum_counts2, count_words2);

    // sum_counts1 \
    //              |-> sum_counts2 -> count_words2
    // count_words1/

    let expected = vec![
      super::ATask::SumCounts,
      super::ATask::CountWords,
      super::ATask::SumCounts,
      super::ATask::CountWords,
    ];

    for (idx, task) in task_pipeline.into_iter().enumerate() {
      assert_eq!(expected[idx], task);
    }
  }
}
