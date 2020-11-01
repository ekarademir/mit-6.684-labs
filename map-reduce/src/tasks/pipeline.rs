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
use log::{debug, info};

use super::task_assignment::{
  ATask,
  TaskAssignment,
  TaskInput,
  TaskInputs,
  TaskKind,
};

const START_KEY: &str = "GRAPH_START";
const REASSIGN_TRESHOLD:Duration = Duration::from_secs(60 * 5); // 5 minutes

#[derive(Debug, PartialEq, Clone)]
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

  fn next_tasks(&self, &idx: &TaskNode) -> Vec<TaskNode> {
    let mut nexts: Vec<TaskNode> = Vec::new();
    for neighbor in self.inner.neighbors_directed(idx, petgraph::Direction::Outgoing) {
      nexts.push(neighbor);
    }
    nexts
  }

  fn is_at_end(&self, &idx: &TaskNode) -> bool {
    let nexts = self.next_tasks(&idx);
    nexts.len() == 0
  }

  pub fn build(mut self) -> Pipeline {
    let reversed = {
      let mut a = self.linearize();
      a.reverse();
      a
    };
    let final_task = self.add_task(ATask::FinalTask);
    for task in reversed {
      if self.is_at_end(&task) {
        self.order_tasks(task, final_task);
      }
    }
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

#[derive(Debug)]
pub enum PipelineError {
  TaskIdNotFound,
  KeyNotFound,
  TaskInputNotFound,
  CantReadStore,
  ResultKeyMissing,
  MalformedTaskInputs,
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

// Kind of Option but with three outcomes
#[derive(Debug, PartialEq)]
pub enum NextTask {
  Ready(TaskAssignment),
  Waiting,
  Finished,
}

// TODO Refactor: from below impl it looks like TaskStore should be another struct with lookups etc.
impl Pipeline {
  pub fn new() -> PipelineBuilder {
    PipelineBuilder::new()
  }

  pub fn finished_task(
    &mut self,
    task_id: u32,
    key: String,
    finished: TaskInput,
    maybe_result_key: Option<String>,
    maybe_result: Option<TaskInput>  // At least, final task will not give a result
  ) -> Result<(), PipelineError> {
    let task_id = graph::NodeIndex::new(task_id as usize);
    // Check if there is a task input in store
    match self.get_task_input_status(task_id, key.clone(), finished.clone()) {
      Err(x) => return Err(x),
      _ => {}
    }
    // Update the old entry
    self.upsert_status(task_id, key, finished, AssignmentStatus::Finished);
    if let Some(result) = maybe_result {
      // There is some resulting file, insert new entry to all subsequent tasks
      if let Some(result_key) = maybe_result_key {
        for next_task in self.next_tasks(&task_id) {
          self.upsert_status(
            next_task,
            result_key.clone(),
            result.clone(),
            AssignmentStatus::Unassigned
          );
        }
      } else {
        // Result file is provided but result key is missing
        return Err(PipelineError::ResultKeyMissing);
      }
    }
    Ok(())
  }

  pub fn update_assignment(
    &mut self,
    task_id: TaskNode,
    key: String,
    inputs: TaskInputs,
  ) -> Result<(), PipelineError> {
    // There should at least be 1 element in inputs
    if inputs.len() == 0 {
      return Err(PipelineError::MalformedTaskInputs);
    }
    // For reduce tasks all inputs will have the same status
    // For map tasks there is only one input
    // So in either case lets check if there is an entry
    match self.get_task_input_status(task_id, key.clone(), inputs[0].clone()) {
      Err(x) => return Err(x),
      _ => {}
    }

    // Now that we know there is an entry, lets update their status.
    let status = AssignmentStatus::Assigned(Instant::now()); // So that reduce inputs share the same instant
    for input in inputs {
      self.upsert_status(task_id, key.clone(), input, status.clone());
    }

    Ok(())
  }

  pub fn is_finished(&self) -> bool {
    NextTask::Finished == self.next()
  }

  pub fn next(&self) -> NextTask {
    if let Ok(store) = self.store.read() {
      for (node_idx, key_store) in store.iter() {
        debug!("Checking task {:?} whihc is a {:?}", self[node_idx], self[node_idx].kind());
        // We try to optimize to finish map tasks as soon as possible,
        // so check if there are unfinished map tasks and return them first
        match self[node_idx].kind() {
          TaskKind::Map => {
            // If this is a map task, we assign one input to one function
            // find the next unfinished task input
            for (key, input_store) in key_store.iter() {
              for (task_input, status) in input_store.iter() {
                match status {
                  AssignmentStatus::Unassigned => {
                    debug!{"Found input {:?} for key '{:?}' is not assigned yet", task_input, key};
                    let assignment = TaskAssignment {
                      task: self[node_idx],
                      input: vec![task_input.clone()],
                      task_id: node_idx.index() as u32,
                      key: key.clone(),
                    };
                    return NextTask::Ready(assignment);
                  }, // AssignmentStatus::Unassigned
                  AssignmentStatus::Assigned(when) => {
                    if Instant::now().duration_since(*when) > REASSIGN_TRESHOLD {
                      let assignment = TaskAssignment {
                        task: self[node_idx],
                        input: vec![task_input.clone()],
                        task_id: node_idx.index() as u32,
                        key: key.clone(),
                      };
                      return NextTask::Ready(assignment);
                    } // If reassign threshold is not passed do nothing
                  }, // AssignmentStatus::Assigned
                  AssignmentStatus::Finished => {} // Do nothing
                } // match status
              } // input_store loop
            } // key_store loop
          }, // TaskKind::Map
          TaskKind::Reduce => {
            // If this is a reduce task, we need to make sure all the inputs are produced by
            // previous task tree. This also means that a single branch of a Reduce task can halt
            // execution of the pipeline unnecessarily but it's ok. We go breadth first but still
            // wait any Reduce
            if self.have_parents_finished(&node_idx) {
              debug!{"All parents of this reduce node have finished"};
              // For reduce tasks, we assign all inputs for a key
              // Since we are waiting for parents before assigning anything, all inputs are
              // Unassigned, or assigned to the same task (to reduce), or finished at the same time
              for (key, input_store) in key_store.iter() {
                // Below is a bit hacky but should do the job. We can probably find a way to not to
                // loop twice.
                // Ideally the following loop shouldn't run more than one ieteration.
                debug!("Checking if this task has been assigned or finished before");
                for (task_input, status) in input_store.iter() {
                  // If any of the task_inputs finished or are not to be reassigned, then there
                  // is nothing to be reassigned. Keep waiting.
                  match status {
                    AssignmentStatus::Finished => return NextTask::Waiting,
                    AssignmentStatus::Assigned(t) => if Instant::now().duration_since(*t) <= REASSIGN_TRESHOLD {
                      debug!("Reassignment threahold has not passed");
                      return NextTask::Waiting;
                    },
                    _ => break,
                  }
                }
                debug!("Collecting inputs");
                // If we reach here, we can create the assignment, now loop for actually collecting
                // the documents
                let inputs = input_store.keys()
                  .fold(TaskInputs::new(), |acc, task_input| {
                    acc.push(task_input.clone());
                    acc
                  });
                debug!{"Found {:?} inputs for key '{:?}' is not assigned yet", inputs.len(), key};
                let assignment = TaskAssignment {
                  task: self[node_idx],
                  input: inputs,
                  task_id: node_idx.index() as u32,
                  key: key.clone(),
                };
                return NextTask::Ready(assignment);
              }
            } else {
              debug!{"This reduce node has unfinished parents, halt execution until they finish."};
              return NextTask::Waiting;
            }
          }, // TaskKind::Reduce
          TaskKind::Final => {
            // If this is the final task, we just need to make sure if all parents have finished.
            // If all parents fnished then there is no firther assignment. Pipeline is finished.
            if self.have_parents_finished(&node_idx) {
              debug!("Pipeline finished");
              return NextTask::Finished;
            } else {
              debug!("Still waiting final task");
              return NextTask::Waiting;
            }
          } //TaskKind::Final
        } // match task kind
      }
    } else {
      debug!("Can't get read lock to see next task");
      return NextTask::Waiting;
    }
    unimplemented!()
  }

  // TODO Make return type a Result
  pub fn init(&mut self, pipeline_inputs: TaskInputs) {
    for task_idx in self.ordered.clone() {
      if self.is_at_start(&task_idx) {
        self.init_task(
          task_idx.clone(),
          START_KEY.to_string(),
          pipeline_inputs.clone()
        );
      }
    }
  }

  fn reduce_task_inputs(&self, &task_id: &TaskNode, key: String) -> TaskInputs {
    let mut inputs = TaskInputs::new();
    let store = self.store.read().unwrap();
    if let Some(key_store) = store.get(&task_id) {
      if let Some(input_store) = key_store.get(&key) {
        for input in input_store.keys() {
          inputs.push(input.clone());
        }
      }
    }
    inputs
  }

  // TODO task check can be optimized with some caching
  fn have_parents_finished(&self, &task_id: &TaskNode) -> bool {
    for parent in self.previous_tasks(&task_id) {
      if !self.task_finished(&parent) {
        return false;
      }
    }
    true
  }

  // TODO task check can be optimized with some caching
  fn task_finished(&self, &task_id: &TaskNode) -> bool {
    // Task is finished when itself and all parents finished
    // For each parent see if task is finished for that parent
    for parent in self.previous_tasks(&task_id) {
      return self.task_finished(&parent);
    }

    // Now check if my tasks are finished
    if let Ok(store) = self.store.read() {
      if let Some(key_store) = store.get(&task_id) {
        // Scan all inputs in all key stores
        for input_store in key_store.values() {
          for input in input_store.values() {
            match input {
              AssignmentStatus::Finished => {},
              // If there are Assigned and Unassigned tasks this task is not finished
              _ => return false,
            }
          }
        }
        // All inputs in the key stores are finished so this task is finished
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  fn get_task_input_status(
    &self,
    task_id: TaskNode,
    key: String,
    task_input: TaskInput
  ) -> Result<AssignmentStatus, PipelineError> {
    if let Ok(store) = self.store.read() {
      if let Some(key_store) = store.get(&task_id) {
        if let Some(input_store) = key_store.get(&key) {
          if let Some(assignment_status) = input_store.get(&task_input) {
            Ok(assignment_status.clone())
          } else {
            return Err(PipelineError::TaskInputNotFound);
          }
        } else {
          return Err(PipelineError::KeyNotFound);
        }
      } else {
        return Err(PipelineError::TaskIdNotFound);
      }
    } else {
      return Err(PipelineError::CantReadStore);
    }
  }

  // TODO Make return type a Result
  fn init_task(&mut self, task_idx: TaskNode, key: String, task_inputs: TaskInputs) {
    for input in task_inputs {
      self.upsert_status(
        task_idx.clone(),
        key.clone(),
        input,
        AssignmentStatus::Unassigned
      );
    }
  }

  // TODO Make return type a Result
  fn upsert_status(
    &mut self,
    task_id: TaskNode,
    key: String,
    input: TaskInput,
    status: AssignmentStatus
  ) {
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
  // #[test]
  // fn test_linear_pipeline() {
  //   // task1 -> task2 -> end
  //   let mut test_pipeline = super::Pipeline::new();
  //   let task1 = test_pipeline.add_task(super::ATask::CountWords);
  //   let task2 = test_pipeline.add_task(super::ATask::SumCounts);
  //   test_pipeline.order_tasks(task1, task2);
  //   let mut task_pipeline = test_pipeline.build();

  //   // Form inputs
  //   let pipeline_input1 = super::TaskInput {
  //     machine_addr: "http://some.machine".to_string(),
  //     file: "some_file1.txt".to_string(),
  //   };
  //   let pipeline_input2 = super::TaskInput {
  //     machine_addr: "http://other.machine".to_string(),
  //     file: "some_file2.txt".to_string(),
  //   };
  //   let pipeline_inputs = vec![
  //     pipeline_input1.clone(),
  //     pipeline_input2.clone(),
  //   ];

  //   // Form results
  //   let task_result1 = super::TaskInput {
  //     machine_addr: "http://some.machine".to_string(),
  //     file: "result_file1.txt".to_string(),
  //   };
  //   let task_result2 = super::TaskInput {
  //     machine_addr: "http://other.machine".to_string(),
  //     file: "result_file2.txt".to_string(),
  //   };

  //   // Initiate pipeline which will populate the first task
  //   task_pipeline.init(pipeline_inputs.clone());

  //   let start_key = super::START_KEY.to_string();

  //   let expected = super::TaskAssignment {
  //     task: super::ATask::CountWords,
  //     input: task_inputs.clone(),
  //     task_id: task1.index() as u32,
  //   };

  //   // Only one task in the pipeline
  //   assert_eq!(task_pipeline.next(), Some(expected));
  //   // Simulate task finish
  //   match task_pipeline.finished_task(
  //     task1.index() as u32,
  //     key.clone(),
  //     task_input.clone(),
  //     None,
  //     None
  //   ) {
  //     Ok(()) => assert!(true),
  //     Err(_) => assert!(false) // Shouldn't reach this branch
  //   }
  //   // No task left
  //   assert!(task_pipeline.is_finished());
  // }

  #[test]
  fn test_single_task() {
    // A pipeline with just one task and one input
    let mut test_pipeline = super::Pipeline::new();
    let task1 = test_pipeline.add_task(super::ATask::CountWords);
    let mut task_pipeline = test_pipeline.build();

    // Form inputs
    let task_input = super::TaskInput {
      machine_addr: "http://some.machine".to_string(),
      file: "some_file.txt".to_string(),
    };
    let task_inputs = vec![task_input.clone()];

    // Kick off pipeline
    task_pipeline.init(task_inputs.clone());

    let key = super::START_KEY.to_string();

    let expected = super::TaskAssignment {
      task: super::ATask::CountWords,
      input: task_inputs.clone(),
      task_id: task1.index() as u32,
    };

    // Only one task in the pipeline
    assert_eq!(task_pipeline.next(), super::NextTask::Ready(expected));
    // Simulate task finish
    match task_pipeline.finished_task(
      task1.index() as u32,
      key.clone(),
      task_input.clone(),
      None,
      None
    ) {
      Ok(()) => assert!(true),
      Err(_) => assert!(false) // Shouldn't reach this branch
    }
    // No task left
    assert!(task_pipeline.is_finished());
  }

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

    // Init should populate store immediatelly
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

    assert_eq!(task_pipeline.is_at_end(&task3), false); // Final task will be implicitly added to the end
    assert_eq!(task_pipeline.is_at_start(&task1), true);
    assert_eq!(task_pipeline.is_at_start(&task2), false);
    assert_eq!(task_pipeline.is_at_end(&task1), false);
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
