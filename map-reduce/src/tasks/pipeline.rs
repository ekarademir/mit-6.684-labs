use std::ops::Index;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::thread;

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

const START_KEY: &str = "GRAPH_START";
const REASSIGN_TRESHOLD:Duration = Duration::from_secs(60 * 5); // 5 minutes

#[derive(Debug, PartialEq)]
enum AssignmentStatus {
  Unassigned,
  Assigned(Instant),
  Finished
}

impl AssignmentStatus {
  pub fn assigned() -> AssignmentStatus {
    AssignmentStatus::Assigned(Instant::now())
  }
}


pub type TaskNode = graph::NodeIndex;
type TaskGraph = petgraph::Graph<ATask, (), petgraph::Directed>;
type InputStore = HashMap<TaskInput, AssignmentStatus>;
type KeyStore = HashMap<String, InputStore>; // Values are task inputs to status of task
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

impl Index<&TaskNode> for Pipeline {
  type Output = ATask;

  fn index(&self, &index: &TaskNode) -> &Self::Output {
      &self.inner[index]
  }
}

impl Pipeline {
  pub fn new() -> PipelineBuilder {
    PipelineBuilder::new()
  }

  pub fn finished_task(&mut self, task_id: i32, key: String, result: TaskInput) {
    // find the tasknode and marking the task input done
    // Then add the taskinput as undone to the next level of tasks
    unimplemented!();
  }

  pub fn is_finished(&self) -> bool {
    // Establish finish case
    unimplemented!();
  }

  // TODO Add test
  pub fn next(&self) -> Option<TaskAssignment> {
    for (task_idx, key_store) in self.store.read().unwrap().iter() {
      for (key, input_store) in key_store.iter() {
        for (task_input, status) in input_store.iter() {
          match status {
            AssignmentStatus::Unassigned => {
              let task = self[task_idx];
              let input = vec![task_input.clone()];
              let task_id = task_idx.index() as i32;
              return Some(
                TaskAssignment {task, input, task_id,}
              );
            },
            AssignmentStatus::Assigned(t) => {
              if Instant::now().duration_since(*t) > REASSIGN_TRESHOLD {
                let task = self[task_idx];
                let input = vec![task_input.clone()];
                let task_id = task_idx.index() as i32;
                return Some(
                  TaskAssignment {task, input, task_id,}
                );
              }
            },
            _ => continue
          }
        }
      }
    }
    None
  }

  pub fn init(&mut self, pipeline_inputs: TaskInputs) {
    for task_idx in self.ordered.clone() {
      if self.is_at_start(&task_idx) {
        for input in pipeline_inputs.clone() {
          self.upsert_status(
            task_idx.clone(),
            START_KEY.to_string(),
            input.clone(),
            AssignmentStatus::Unassigned
          );
        }
      }
    }
  }

  fn upsert_status(&mut self, task_id: TaskNode, key: String, input: TaskInput, status: AssignmentStatus) {
    // Store -[TaskNode]-> KeyStore -[Key]-> InputStore -[TaskInput]-> Status
    // If any key in the nested map is not there create the entry
    loop {
      if let Ok(mut store) = self.store.try_write() {
        let mut key_store = match store.remove(&task_id) {
          Some(x) => x,
          None => KeyStore::new(),
        };

        let mut input_store = match key_store.remove(&key) {
          Some(x) => x,
          None => InputStore::new(),
        };

        // Last nested level, just dump the content and replace
        let _ = input_store.remove(&input);

        // Now nest them back
        input_store.insert(input, status);
        key_store.insert(key, input_store);
        store.insert(task_id, key_store);

        // Break the wait loop
        break;
      } else {
        thread::sleep(Duration::from_micros(150));
      }
    }
  }


  fn is_at_start(&self, &idx: &TaskNode) -> bool {
    let prevs = self.previous_tasks(&idx);
    prevs.len() == 0
  }

  fn is_at_end(&self, &idx: &TaskNode) -> bool {
    let nexts = self.next_tasks(&idx);
    nexts.len() == 0
  }

  fn previous_tasks(&self, &idx: &TaskNode) -> Vec<TaskNode> {
    let mut prevs: Vec<TaskNode> = Vec::new();
    for neighbor in self.inner.neighbors_directed(idx, petgraph::Direction::Incoming) {
      prevs.push(neighbor);
    }
    prevs
  }

  fn next_tasks(&self, &idx: &TaskNode) -> Vec<TaskNode> {
    let mut nexts: Vec<TaskNode> = Vec::new();
    for neighbor in self.inner.neighbors_directed(idx, petgraph::Direction::Outgoing) {
      nexts.push(neighbor);
    }
    nexts
  }
}


#[cfg(test)]
mod tests {
  #[test]
  fn test_init() {
    let mut test_pipeline = super::Pipeline::new();
    let task1 = test_pipeline.add_task(super::ATask::CountWords);
    let task2 = test_pipeline.add_task(super::ATask::CountWords);
    let task3 = test_pipeline.add_task(super::ATask::CountWords);

    // Two starting tasks
    test_pipeline.order_tasks(task1, task3);
    test_pipeline.order_tasks(task2, task3);

    let mut task_pipeline = test_pipeline.build();

    let task_input = super::TaskInput {
      machine_addr: "http://some.machine".to_string(),
      file: "some_file.txt".to_string(),
    };
    let task_inputs = vec![task_input.clone()];

    // Init shoud populate store immediatelly
    task_pipeline.init(task_inputs);

    let key = super::START_KEY.to_string();

    assert!(
      task_pipeline
        .store.read().unwrap()
        .get(&task1).unwrap()
        .get(&key).unwrap()
        .get(&task_input).unwrap()
        .eq(&super::AssignmentStatus::Unassigned)
    );

    assert!(
      task_pipeline
        .store.read().unwrap()
        .get(&task2).unwrap()
        .get(&key).unwrap()
        .get(&task_input).unwrap()
        .eq(&super::AssignmentStatus::Unassigned)
    );
  }

  #[test]
  fn test_upsert_status() {
    let mut test_pipeline = super::Pipeline::new();
    let task1 = test_pipeline.add_task(super::ATask::CountWords);
    let task2 = test_pipeline.add_task(super::ATask::CountWords);
    let task3 = test_pipeline.add_task(super::ATask::CountWords);
    test_pipeline.order_tasks(task1, task2);
    test_pipeline.order_tasks(task2, task3);
    let mut task_pipeline = test_pipeline.build();

    let task_input = super::TaskInput {
      machine_addr: "http://some.machine".to_string(),
      file: "some_file.txt".to_string(),
    };

    let key = "test_key".to_string();

    task_pipeline.upsert_status(
      task1.clone(),
      key.clone(),
      task_input.clone(),
      super::AssignmentStatus::Unassigned,
    );

    // First nest
    assert!(
      task_pipeline
        .store.read().unwrap()
        .contains_key(&task1)
    );

    // Second nest
    assert!(
      task_pipeline
        .store.read().unwrap()
        .get(&task1).unwrap()
        .contains_key(&key)
    );

    // Last nest
    assert!(
      task_pipeline
        .store.read().unwrap()
        .get(&task1).unwrap()
        .get(&key).unwrap()
        .contains_key(&task_input)
    );

    // The Gem
    assert!(
      task_pipeline
        .store.read().unwrap()
        .get(&task1).unwrap()
        .get(&key).unwrap()
        .get(&task_input).unwrap()
        .eq(&super::AssignmentStatus::Unassigned)
    );

    // Update
    task_pipeline.upsert_status(
      task1.clone(),
      key.clone(),
      task_input.clone(),
      super::AssignmentStatus::Finished,
    );

    // Test again
    // First nest
    assert!(
      task_pipeline
        .store.read().unwrap()
        .contains_key(&task1)
    );

    // Second nest
    assert!(
      task_pipeline
        .store.read().unwrap()
        .get(&task1).unwrap()
        .contains_key(&key)
    );

    // Last nest
    assert!(
      task_pipeline
        .store.read().unwrap()
        .get(&task1).unwrap()
        .get(&key).unwrap()
        .contains_key(&task_input)
    );

    // The Gem
    assert!(
      task_pipeline
        .store.read().unwrap()
        .get(&task1).unwrap()
        .get(&key).unwrap()
        .get(&task_input).unwrap()
        .eq(&super::AssignmentStatus::Finished)
    );
  }

  #[test]
  fn test_order_check() {
    let mut test_pipeline = super::Pipeline::new();
    let task1 = test_pipeline.add_task(super::ATask::CountWords);
    let task2 = test_pipeline.add_task(super::ATask::CountWords);
    let task3 = test_pipeline.add_task(super::ATask::CountWords);
    test_pipeline.order_tasks(task1, task2);
    test_pipeline.order_tasks(task2, task3);
    let task_pipeline = test_pipeline.build();

    assert!(task_pipeline.is_at_end(&task3));
    assert!(task_pipeline.is_at_start(&task1));
    assert_ne!(task_pipeline.is_at_end(&task1), true);
    assert_ne!(task_pipeline.is_at_start(&task2), true);
  }

  #[test]
  fn test_builder_linear_dag() {
    let mut task_pipeline_builder = super::PipelineBuilder::new();
    let count_words = task_pipeline_builder.add_task(super::ATask::CountWords);
    let sum_counts = task_pipeline_builder.add_task(super::ATask::SumCounts);

    task_pipeline_builder.order_tasks(count_words, sum_counts);

    // count_words -> sum_counts


    let expected = vec![
      super::ATask::CountWords,
      super::ATask::SumCounts
    ];

    for (idx, task_node) in task_pipeline_builder.linearize().iter().enumerate() {
      assert_eq!(expected[idx], task_pipeline_builder[task_node]);
    }
  }

  #[test]
  fn test_builder_cyclic() {
    let mut task_pipeline_builder = super::Pipeline::new();
    let count_words = task_pipeline_builder.add_task(super::ATask::CountWords);
    let sum_counts = task_pipeline_builder.add_task(super::ATask::SumCounts);

    task_pipeline_builder.order_tasks(count_words, sum_counts);
    task_pipeline_builder.order_tasks(sum_counts, count_words);

    // count_words -> sum_counts
    //            \__/

    let expected: Vec<super::ATask> = Vec::new();

    for (idx, task_node) in task_pipeline_builder.linearize().iter().enumerate() {
      assert_eq!(expected[idx], task_pipeline_builder[task_node]);
    }
  }

  #[test]
  fn test_builder_multi_level_dag() {
    let mut task_pipeline_builder = super::Pipeline::new();
    let count_words1 = task_pipeline_builder.add_task(super::ATask::CountWords);
    let sum_counts1 = task_pipeline_builder.add_task(super::ATask::SumCounts);
    let count_words2 = task_pipeline_builder.add_task(super::ATask::CountWords);
    let sum_counts2 = task_pipeline_builder.add_task(super::ATask::SumCounts);

    task_pipeline_builder.order_tasks(sum_counts1, sum_counts2);
    task_pipeline_builder.order_tasks(count_words1, sum_counts2);
    task_pipeline_builder.order_tasks(sum_counts2, count_words2);

    // sum_counts1 \
    //              |-> sum_counts2 -> count_words2
    // count_words1/

    let expected = vec![
      super::ATask::SumCounts,
      super::ATask::CountWords,
      super::ATask::SumCounts,
      super::ATask::CountWords,
    ];

    for (idx, task_node) in task_pipeline_builder.linearize().iter().enumerate() {
      assert_eq!(expected[idx], task_pipeline_builder[task_node]);
    }
  }
}
