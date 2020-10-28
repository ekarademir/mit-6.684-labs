use std::ops::Index;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
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
type TaskStoreInner = HashMap<TaskNode, KeyStore>;
type TaskStore = Arc<RwLock<TaskStoreInner>>;

pub struct TaskRun {
  task_assignment: TaskAssignment,

}

pub struct PipelineBuilder {
  inner: TaskGraph,
}

impl Index<&TaskNode> for PipelineBuilder {
  type Output = ATask;

  fn index(&self, &index: &TaskNode) -> &Self::Output {
      &self.inner[index]
  }
}

impl PipelineBuilder {
  pub fn new() -> Self {
    let inner = TaskGraph::new();

    PipelineBuilder {
      inner,
    }
  }

  pub fn add_task(&mut self, task: ATask) -> TaskNode {
    self.inner.add_node(task)
  }

  pub fn order_tasks(&mut self, from: TaskNode, to: TaskNode) {
    // TODO catch cyclic ordering and maybe throw error
    self.inner.add_edge(from, to, ());
  }

  fn linearize(&self) -> Vec<TaskNode> {
    let mut linear: Vec<TaskNode> = Vec::new();

    let mut topological_sort = visit::Topo::new(&self.inner);
    while let Some(node_idx) = topological_sort.next(&self.inner) {
      linear.push(node_idx);
    }

    linear
  }

  // pub fn get_item(&self, &idx: &TaskNode) -> ATask {
  //   self.inner[idx].clone()
  // }

  pub fn build(self) -> Pipeline {
    let ordered = self.linearize();
    let store = Arc::new(
      RwLock::new(
        TaskStoreInner::new()
      )
    );
    Pipeline {
      ordered,
      store,
      inner: self.inner,
    }
  }
}

pub struct Pipeline {
  inner: TaskGraph,
  store: TaskStore,
  ordered: Vec<TaskNode>,
}

impl Pipeline {
  pub fn new() -> PipelineBuilder {
    PipelineBuilder::new()
  }

  pub fn finished_task(&mut self, task_id: TaskNode, key: String, result: TaskInput) {
    // find the tasknode and marking the task input done
    // Then add the taskinput as undone to the next level of tasks
    unimplemented!();
  }

  pub fn next() -> Option<TaskAssignment> {
    // Return the next item in assignment queue
    // Scan the store get the next unfinished task input
    //    If passed tolerated amount of time assign again
    // If all the task input of the last level are finished then we are done so return None
    unimplemented!()
  }

  pub fn init(&mut self, pipeline_inputs: TaskInputs) {

    //  Files for the first one probably should be acquired from the master
    // Insert inputs to task store with the first line of tasks in the graph
    unimplemented!()
  }
}


#[cfg(test)]
mod tests {
  #[test]
  fn test_builder_linear_dag() {
    let mut task_pipeline = super::PipelineBuilder::new();
    let count_words = task_pipeline.add_task(super::ATask::CountWords);
    let sum_counts = task_pipeline.add_task(super::ATask::SumCounts);

    task_pipeline.order_tasks(count_words, sum_counts);

    // count_words -> sum_counts


    let expected = vec![
      super::ATask::CountWords,
      super::ATask::SumCounts
    ];

    for (idx, task_node) in task_pipeline.linearize().iter().enumerate() {
      assert_eq!(expected[idx], task_pipeline[task_node]);
    }
  }

  #[test]
  fn test_builder_cyclic() {
    let mut task_pipeline = super::Pipeline::new();
    let count_words = task_pipeline.add_task(super::ATask::CountWords);
    let sum_counts = task_pipeline.add_task(super::ATask::SumCounts);

    task_pipeline.order_tasks(count_words, sum_counts);
    task_pipeline.order_tasks(sum_counts, count_words);

    // count_words -> sum_counts
    //            \__/

    let expected: Vec<super::ATask> = Vec::new();

    for (idx, task_node) in task_pipeline.linearize().iter().enumerate() {
      assert_eq!(expected[idx], task_pipeline[task_node]);
    }
  }

  #[test]
  fn test_builder_multi_level_dag() {
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

    for (idx, task_node) in task_pipeline.linearize().iter().enumerate() {
      assert_eq!(expected[idx], task_pipeline[task_node]);
    }
  }
}
