#!/usr/bin/env python3
"""
Tests for ParallelRunner and SequentialRunner classes from experiment.py
"""

import unittest
import logging
import threading
import time
from unittest.mock import Mock

# Import the classes to test
import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from experiment import ParallelRunner, SequentialRunner


class MockExperimentEngine:
    """Mock experiment engine for testing"""
    def __init__(self, exp_id: str, delay: float = 0):
        self.exp = Mock()
        self.exp.id = exp_id
        self.delay = delay
        self.was_processed = False
        self.process_lock = threading.Lock()

    def process(self):
        """Simulate processing"""
        if self.delay > 0:
            time.sleep(self.delay)
        with self.process_lock:
            self.was_processed = True


class TestParallelRunner(unittest.TestCase):
    """Test cases for ParallelRunner class"""

    def setUp(self):
        """Set up test fixtures"""
        self.logger = logging.getLogger('test')
        self.logger.setLevel(logging.DEBUG)

    def tearDown(self):
        """Clean up after tests"""
        # Ensure any runners are properly cleaned up
        pass

    def test_step_with_single_experiment(self):
        """Test step execution with a single experiment"""
        runner = ParallelRunner(max_workers=2, name="test_runner", logger=self.logger)

        exp_engine = MockExperimentEngine("exp_1")
        step_func = Mock(side_effect=lambda e: e.process())

        runner.step([exp_engine], step_func)

        # Give threads time to execute
        time.sleep(0.5)

        # Verify the step function was called
        step_func.assert_called_once()
        self.assertTrue(exp_engine.was_processed)

    def test_step_with_multiple_experiments(self):
        """Test step execution with multiple experiments"""
        runner = ParallelRunner(max_workers=3, name="test_runner", logger=self.logger)

        exp_engines = [
            MockExperimentEngine(f"exp_{i}")
            for i in range(3)
        ]
        step_func = Mock(side_effect=lambda e: e.process())

        runner.step(exp_engines, step_func)

        # Give threads time to execute
        time.sleep(0.5)

        # Verify all experiments were processed
        self.assertEqual(step_func.call_count, 3)
        for exp_engine in exp_engines:
            self.assertTrue(exp_engine.was_processed)

    def test_worker_pool_limits(self):
        """Test that ParallelRunner respects max_workers limit"""
        max_workers = 2
        runner = ParallelRunner(max_workers=max_workers, name="test_runner", logger=self.logger)

        # Create more experiments than workers
        exp_engines = [
            MockExperimentEngine(f"exp_{i}", delay=0.2)
            for i in range(5)
        ]

        step_func = Mock(side_effect=lambda e: e.process())

        # First step should submit max_workers experiments
        runner.step(exp_engines, step_func)

        # Check that only max_workers are in flight
        self.assertLessEqual(len(runner.exec_state), max_workers)

        # Remaining experiments should be skipped in this step
        submitted_count = len(runner.exec_state)
        self.assertEqual(submitted_count, min(max_workers, len(exp_engines)))

    def test_worker_reuse_across_steps(self):
        """Test that workers are reused across multiple steps"""
        runner = ParallelRunner(max_workers=2, name="test_runner", logger=self.logger)

        # First step with 2 experiments
        exp_engines_1 = [
            MockExperimentEngine("exp_1", delay=0.1),
            MockExperimentEngine("exp_2", delay=0.1)
        ]
        step_func = Mock(side_effect=lambda e: e.process())

        runner.step(exp_engines_1, step_func)
        self.assertEqual(len(runner.exec_state), 2)

        # Wait for completion
        time.sleep(0.5)

        # Verify all processed and state cleared
        self.assertEqual(len(runner.exec_state), 0)
        for exp_engine in exp_engines_1:
            self.assertTrue(exp_engine.was_processed)

        # Second step with different experiments
        exp_engines_2 = [
            MockExperimentEngine("exp_3"),
            MockExperimentEngine("exp_4")
        ]
        step_func.reset_mock()

        runner.step(exp_engines_2, step_func)

        # Wait for completion
        time.sleep(0.3)

        # Verify new experiments were processed
        for exp_engine in exp_engines_2:
            self.assertTrue(exp_engine.was_processed)

    def test_exception_handling(self):
        """Test that exceptions in step function are handled gracefully"""
        runner = ParallelRunner(max_workers=3, name="test_runner", logger=self.logger)

        exp_engines = [
            MockExperimentEngine("exp_1"),
            MockExperimentEngine("exp_2"),
            MockExperimentEngine("exp_3")
        ]

        call_count = [0]
        def step_func_with_exception(e):
            call_count[0] += 1
            if "exp_1" in e.exp.id:
                raise ValueError("Test exception")
            e.process()

        step_func = Mock(side_effect=step_func_with_exception)

        # This should not raise an exception
        runner.step(exp_engines, step_func)

        time.sleep(0.5)

        # All experiments should still be attempted
        self.assertEqual(step_func.call_count, 3)

        # Ensure experiment 1 was still submitted despite exception
        self.assertFalse(exp_engines[0].was_processed)  # Failed due to exception
        self.assertTrue(exp_engines[1].was_processed)
        self.assertTrue(exp_engines[2].was_processed)

    def test_state_cleanup_after_experiment(self):
        """Test that exec_state is cleaned up after experiment completes"""
        runner = ParallelRunner(max_workers=2, name="test_runner", logger=self.logger)

        exp_engine = MockExperimentEngine("exp_1", delay=0.1)
        step_func = Mock(side_effect=lambda e: e.process())

        runner.step([exp_engine], step_func)

        # State should have one entry immediately after step
        self.assertEqual(len(runner.exec_state), 1)
        self.assertIn("exp_1", runner.exec_state)

        # Wait for completion
        time.sleep(0.5)

        # State should be cleaned up
        self.assertEqual(len(runner.exec_state), 0)
        self.assertNotIn("exp_1", runner.exec_state)

    def test_concurrent_experiment_processing(self):
        """Test that experiments are processed concurrently"""
        runner = ParallelRunner(max_workers=3, name="test_runner", logger=self.logger)

        # Create experiments with delays
        exp_engines = [
            MockExperimentEngine(f"exp_{i}", delay=0.2)
            for i in range(3)
        ]

        step_func = Mock(side_effect=lambda e: e.process())
        runner.step(exp_engines, step_func)

        # Now measure the actual completion time
        start_time = time.time()

        # Wait for all experiments to complete
        while len(runner.exec_state) > 0 and time.time() - start_time < 2.0:
            time.sleep(0.05)

        elapsed = time.time() - start_time

        # If concurrent: should be ~0.2s (3 tasks of 0.2s run in parallel)
        # If sequential: should be ~0.6s (3 tasks of 0.2s run one by one)
        # We expect concurrent, so elapsed should be < 0.4s
        self.assertLess(elapsed, 0.4,
                       f"Execution took {elapsed}s, suggests sequential execution")

        # Verify all experiments were processed
        for exp_engine in exp_engines:
            self.assertTrue(exp_engine.was_processed)

    def test_skip_if_no_workers_available(self):
        """Test that experiments are skipped if no workers are available"""
        runner = ParallelRunner(max_workers=1, name="test_runner", logger=self.logger)

        # Create two slow experiments
        exp_engines = [
            MockExperimentEngine("exp_1", delay=0.3),
            MockExperimentEngine("exp_2", delay=0.3)
        ]

        step_func = Mock(side_effect=lambda e: e.process())

        # Step with first experiment (will occupy the single worker)
        runner.step(exp_engines, step_func)

        # Check that only first experiment was submitted
        self.assertEqual(len(runner.exec_state), 1)
        self.assertIn("exp_1", runner.exec_state)

        # Step with second experiment while first is running
        # It should be skipped due to no available workers
        runner.step([exp_engines[1]], step_func)

        # Second experiment should not be in exec_state
        # (it was skipped because max_workers limit was reached)
        self.assertEqual(len(runner.exec_state), 1)

    def test_duplicate_experiment_not_submitted_twice(self):
        """Test that same experiment is not submitted twice if already running"""
        runner = ParallelRunner(max_workers=2, name="test_runner", logger=self.logger)

        exp_engine = MockExperimentEngine("exp_1", delay=0.2)

        step_func = Mock(side_effect=lambda e: e.process())

        # First step submits the experiment
        runner.step([exp_engine], step_func)
        self.assertEqual(len(runner.exec_state), 1)

        # Immediately step again with same experiment
        runner.step([exp_engine], step_func)

        # Should still only have one submission
        self.assertEqual(len(runner.exec_state), 1)
        # Step function should only be called once
        self.assertEqual(step_func.call_count, 1)


class TestSequentialRunner(unittest.TestCase):
    """Test cases for SequentialRunner class"""

    def setUp(self):
        """Set up test fixtures"""
        self.logger = logging.getLogger('test')
        self.logger.setLevel(logging.DEBUG)

    def test_initialization(self):
        """Test SequentialRunner initialization"""
        runner = SequentialRunner(logger=self.logger)

        self.assertEqual(runner.logger, self.logger)

    def test_step_with_single_experiment(self):
        """Test step execution with a single experiment"""
        runner = SequentialRunner(logger=self.logger)

        exp_engine = MockExperimentEngine("exp_1")
        step_func = Mock(side_effect=lambda e: e.process())

        runner.step([exp_engine], step_func)

        # Verify the step function was called
        step_func.assert_called_once()
        self.assertTrue(exp_engine.was_processed)

    def test_step_with_multiple_experiments(self):
        """Test step execution with multiple experiments"""
        runner = SequentialRunner(logger=self.logger)

        exp_engines = [
            MockExperimentEngine(f"exp_{i}")
            for i in range(3)
        ]
        step_func = Mock(side_effect=lambda e: e.process())

        runner.step(exp_engines, step_func)

        # Verify all experiments were processed in order
        self.assertEqual(step_func.call_count, 3)
        for exp_engine in exp_engines:
            self.assertTrue(exp_engine.was_processed)

    def test_sequential_execution_order(self):
        """Test that experiments are executed sequentially"""
        runner = SequentialRunner(logger=self.logger)

        execution_order = []

        exp_engines = [
            MockExperimentEngine(f"exp_{i}")
            for i in range(3)
        ]

        def step_func(e):
            execution_order.append(e.exp.id)
            e.process()

        step_func_mock = Mock(side_effect=step_func)

        runner.step(exp_engines, step_func_mock)

        # Verify execution order matches input order
        self.assertEqual(execution_order, ["exp_0", "exp_1", "exp_2"])

    def test_exception_handling(self):
        """Test that exceptions are handled and execution continues"""
        runner = SequentialRunner(logger=self.logger)

        exp_engines = [
            MockExperimentEngine("exp_1"),
            MockExperimentEngine("exp_2"),
            MockExperimentEngine("exp_3")
        ]

        call_count = [0]
        def step_func_with_exception(e):
            call_count[0] += 1
            if "exp_1" in e.exp.id:
                raise ValueError("Test exception")
            e.process()

        step_func = Mock(side_effect=step_func_with_exception)

        # This should not raise an exception
        runner.step(exp_engines, step_func)

        # All experiments should still be attempted
        self.assertEqual(step_func.call_count, 3)

        # Experiments after the exception should still be processed
        self.assertFalse(exp_engines[0].was_processed)  # Failed due to exception
        self.assertTrue(exp_engines[1].was_processed)
        self.assertTrue(exp_engines[2].was_processed)


if __name__ == '__main__':
    unittest.main()

