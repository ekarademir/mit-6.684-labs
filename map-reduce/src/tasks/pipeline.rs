use std::iter::IntoIterator;
use std::collections::{VecDeque, HashMap};
use std::time::Instant;

use petgraph::{
  self,
  graph,
  visit,
};

use super::task_assignment::{
  ATask,
  TaskAssignment,
  TaskInput,
  TaskInputs
};

enum AssignmentStatus {
  Assigned(Instant),
  Finished
}

type TaskGraph = petgraph::Graph<ATask, (), petgraph::Directed>;
type TaskNode = graph::NodeIndex;
type KeyStore = HashMap<String, HashMap<TaskInput, AssignmentStatus>>; // Values are task inputs to status of task
type TaskStore = HashMap<TaskNode, KeyStore>;

pub struct TaskRun {
  task_assignment: TaskAssignment,

}

pub struct Pipeline {
  inner: TaskGraph,
  store: TaskStore, // We might need to put this behind a RwLock
}


impl Pipeline {
  pub fn new() -> Self {
    let inner = TaskGraph::new();
    let store = TaskStore::new();
    Pipeline {
      inner,
      store,
    }
  }

  pub fn add_task(&mut self, task: ATask) -> TaskNode {
    self.inner.add_node(task)
  }

  pub fn order_tasks(&mut self, from: TaskNode, to: TaskNode) {
    // TODO catch cyclic ordering and maybe throw error
    self.inner.add_edge(from, to, ());
  }

  pub fn finished_task(&mut self, task_id: TaskNode, key: String, result: TaskInput) {
    // find the tasknode and marking the task input done
    // Then add the taskinput as undone to the next level of tasks
    unimplemented!();
  }

  pub async fn next() -> Option<TaskAssignment> {
    // Return the next item in assignment queue
    // Scan the store get the next unfinished task input
    //    If passed tolerated amount of time assign again
    // If all the task input of the last level are finished then we are done so return None
    unimplemented!()
  }

  pub fn init(&mut self, pipeline_inputs: TaskInputs) {
    // Lock adding stuff
    //  Files for the first one probably should be acquired from the master
    // Insert inputs to task store with the first line of tasks in the graph
    unimplemented!()
  }
}

// TODO this is probably unnecessary
// TODO tests are good fur multi level stuff but order is not important
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
  fn test_linear_dag() {
    let mut task_pipeline = super::Pipeline::new();
    let count_words = task_pipeline.add_task(super::ATask::CountWords);
    let sum_counts = task_pipeline.add_task(super::ATask::SumCounts);

    task_pipeline.order_tasks(count_words, sum_counts);

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
  fn test_cyclic() {
    let mut task_pipeline = super::Pipeline::new();
    let count_words = task_pipeline.add_task(super::ATask::CountWords);
    let sum_counts = task_pipeline.add_task(super::ATask::SumCounts);

    task_pipeline.order_tasks(count_words, sum_counts);
    task_pipeline.order_tasks(sum_counts, count_words);

    // count_words -> sum_counts
    //            \__/

    let expected: Vec<super::ATask> = Vec::new();

    for (idx, task) in task_pipeline.into_iter().enumerate() {
      assert_eq!(expected[idx], task);
    }
  }

  #[test]
  fn test_multi_level_dag() {
    let mut task_pipeline = super::Pipeline::new();
    let count_words1 = task_pipeline.add_task(super::ATask::CountWords);
    let sum_counts1 = task_pipeline.add_task(super::ATask::SumCounts);
    let count_words2 = task_pipeline.add_task(super::ATask::CountWords);
    let sum_counts2 = task_pipeline.add_task(super::ATask::SumCounts);

    task_pipeline.order_tasks(sum_counts1, sum_counts2);
    task_pipeline.order_tasks(count_words1, sum_counts2);
    task_pipeline.order_tasks(sum_counts2, count_words2);

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
