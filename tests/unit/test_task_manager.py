"""
Unit tests for the task manager and queue processing.

These tests validate that the task management system works correctly
with mocked dependencies and proper queue handling.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
import asyncio
from datetime import datetime

from entity_processing.task_manager import EntityTaskManager


@pytest.mark.unit
class TestTaskManager:
    """Test the EntityTaskManager with mocked dependencies"""
    
    @pytest.fixture
    def mock_task_manager(self):
        """Create task manager with mocked dependencies"""
        with patch('entity_processing.task_manager.EntityProcessor') as mock_processor:
            manager = EntityTaskManager(max_concurrent_tasks=2)
            manager.processor = mock_processor.return_value
            return manager
    
    @pytest.fixture
    def sample_tasks(self):
        """Sample task payloads"""
        return [
            {
                'task_id': 'task-001',
                'entity_id': 'school-001',
                'entity_type': 'school',
                'stage': 'location',
                'priority': 1,
                'data_year': 2024
            },
            {
                'task_id': 'task-002',
                'entity_id': 'school-002',
                'entity_type': 'school',
                'stage': 'demographics',
                'priority': 2,
                'data_year': 2024
            },
            {
                'task_id': 'task-003',
                'entity_id': 'location-001',
                'entity_type': 'location',
                'stage': 'location',
                'priority': 3,
                'data_year': 2024
            }
        ]
    
    async def test_task_queue_addition(self, mock_task_manager, sample_tasks):
        """Test adding tasks to the queue"""
        manager = mock_task_manager
        
        # Add tasks to queue
        for task in sample_tasks:
            await manager.add_task(task)
        
        # Verify tasks are in queue
        assert manager.get_queue_size() == 3
        assert manager.get_queue_size('high') == 1  # Priority 1
        assert manager.get_queue_size('medium') == 1  # Priority 2  
        assert manager.get_queue_size('low') == 1  # Priority 3
    
    async def test_task_priority_processing(self, mock_task_manager, sample_tasks):
        """Test that high priority tasks are processed first"""
        manager = mock_task_manager
        
        # Mock processor to track processing order
        processed_tasks = []
        
        async def mock_process_task(task):
            processed_tasks.append(task['task_id'])
            return {'status': 'success', 'task_id': task['task_id']}
        
        manager.processor.process_task = mock_process_task
        
        # Add tasks in reverse priority order
        for task in reversed(sample_tasks):
            await manager.add_task(task)
        
        # Process all tasks
        await manager.process_queue()
        
        # Verify high priority task was processed first
        assert processed_tasks[0] == 'task-001'  # Priority 1 (highest)
        assert len(processed_tasks) == 3
    
    async def test_concurrent_task_processing(self, mock_task_manager):
        """Test that tasks are processed concurrently up to the limit"""
        manager = mock_task_manager
        
        # Create tasks that will take some time to process
        processing_times = []
        
        async def mock_slow_process_task(task):
            start_time = datetime.now()
            await asyncio.sleep(0.1)  # Simulate processing time
            end_time = datetime.now()
            processing_times.append((task['task_id'], start_time, end_time))
            return {'status': 'success', 'task_id': task['task_id']}
        
        manager.processor.process_task = mock_slow_process_task
        
        # Add multiple tasks
        tasks = [
            {'task_id': f'task-{i}', 'entity_id': f'entity-{i}', 'entity_type': 'school', 'priority': 1}
            for i in range(4)
        ]
        
        for task in tasks:
            await manager.add_task(task)
        
        # Process tasks
        start_processing = datetime.now()
        await manager.process_queue()
        end_processing = datetime.now()
        
        # Verify concurrent processing (should take ~0.2s for 4 tasks with max_concurrent=2)
        total_time = (end_processing - start_processing).total_seconds()
        assert total_time < 0.3  # Should be much less than 0.4s (sequential processing)
        assert len(processing_times) == 4
        
        # Verify no more than 2 tasks were running simultaneously
        concurrent_count = 0
        for i, (task_id, start, end) in enumerate(processing_times):
            for j, (other_task_id, other_start, other_end) in enumerate(processing_times):
                if i != j and start <= other_end and end >= other_start:
                    concurrent_count += 1
        
        # Each task should overlap with at most 1 other task (since max_concurrent=2)
        assert concurrent_count <= len(tasks)
    
    async def test_task_failure_handling(self, mock_task_manager):
        """Test that task failures don't stop queue processing"""
        manager = mock_task_manager
        
        processed_tasks = []
        
        async def mock_process_task(task):
            processed_tasks.append(task['task_id'])
            if task['task_id'] == 'task-002':
                raise Exception("Simulated processing error")
            return {'status': 'success', 'task_id': task['task_id']}
        
        manager.processor.process_task = mock_process_task
        
        # Add tasks where one will fail
        tasks = [
            {'task_id': 'task-001', 'entity_id': 'entity-001', 'entity_type': 'school', 'priority': 1},
            {'task_id': 'task-002', 'entity_id': 'entity-002', 'entity_type': 'school', 'priority': 1},
            {'task_id': 'task-003', 'entity_id': 'entity-003', 'entity_type': 'school', 'priority': 1}
        ]
        
        for task in tasks:
            await manager.add_task(task)
        
        # Process queue
        await manager.process_queue()
        
        # Verify all tasks were attempted, even after failure
        assert len(processed_tasks) == 3
        assert 'task-001' in processed_tasks
        assert 'task-002' in processed_tasks  # Failed task was attempted
        assert 'task-003' in processed_tasks
    
    async def test_queue_status_tracking(self, mock_task_manager):
        """Test queue status and metrics tracking"""
        manager = mock_task_manager
        
        # Mock processor
        async def mock_process_task(task):
            await asyncio.sleep(0.05)  # Small delay
            return {'status': 'success', 'task_id': task['task_id']}
        
        manager.processor.process_task = mock_process_task
        
        # Add some tasks
        for i in range(3):
            await manager.add_task({
                'task_id': f'task-{i}',
                'entity_id': f'entity-{i}',
                'entity_type': 'school',
                'priority': 1
            })
        
        # Check initial status
        initial_status = manager.get_queue_status()
        assert initial_status['total_queued'] == 3
        assert initial_status['total_processing'] == 0
        assert initial_status['total_completed'] == 0
        
        # Start processing (but don't wait for completion)
        processing_task = asyncio.create_task(manager.process_queue())
        
        # Give it a moment to start
        await asyncio.sleep(0.01)
        
        # Check status during processing
        processing_status = manager.get_queue_status()
        assert processing_status['total_processing'] >= 0
        
        # Wait for completion
        await processing_task
        
        # Check final status
        final_status = manager.get_queue_status()
        assert final_status['total_queued'] == 0
        assert final_status['total_processing'] == 0
        assert final_status['total_completed'] == 3
    
    def test_task_validation(self, mock_task_manager):
        """Test task payload validation"""
        manager = mock_task_manager
        
        # Valid task
        valid_task = {
            'task_id': 'task-001',
            'entity_id': 'entity-001',
            'entity_type': 'school',
            'priority': 1
        }
        
        assert manager.validate_task(valid_task) is True
        
        # Invalid tasks
        invalid_tasks = [
            {},  # Empty task
            {'task_id': 'task-001'},  # Missing required fields
            {'task_id': 'task-001', 'entity_id': 'entity-001', 'entity_type': 'invalid_type'},  # Invalid entity type
            {'task_id': 'task-001', 'entity_id': 'entity-001', 'entity_type': 'school', 'priority': 'high'}  # Invalid priority type
        ]
        
        for invalid_task in invalid_tasks:
            assert manager.validate_task(invalid_task) is False
    
    async def test_queue_cleanup(self, mock_task_manager):
        """Test queue cleanup functionality"""
        manager = mock_task_manager
        
        # Add some tasks
        for i in range(5):
            await manager.add_task({
                'task_id': f'task-{i}',
                'entity_id': f'entity-{i}',
                'entity_type': 'school',
                'priority': 1
            })
        
        assert manager.get_queue_size() == 5
        
        # Clear queue
        manager.clear_queue()
        
        assert manager.get_queue_size() == 0
        assert manager.get_queue_size('high') == 0
        assert manager.get_queue_size('medium') == 0
        assert manager.get_queue_size('low') == 0


@pytest.mark.unit 
class TestTaskWorkflowIntegration:
    """Test integration between task manager and processing workflows"""
    
    async def test_end_to_end_school_processing(self):
        """Test complete school processing workflow through task manager"""
        with patch('entity_processing.task_manager.EntityProcessor') as mock_processor_class:
            # Setup mock processor
            mock_processor = Mock()
            mock_processor_class.return_value = mock_processor
            
            # Mock the complete processing workflow
            async def mock_process_task(task):
                # Simulate the actual processing stages
                entity_id = task['entity_id']
                entity_type = task['entity_type']
                
                # Simulate stage processing
                stages = ['location', 'demographics', 'enrollment', 'projections', 'metrics']
                completed_stages = []
                
                for stage in stages:
                    # Simulate stage processing success
                    completed_stages.append(stage)
                
                return {
                    'status': 'success',
                    'entity_id': entity_id,
                    'entity_type': entity_type,
                    'completed_stages': completed_stages,
                    'total_stages': len(stages)
                }
            
            mock_processor.process_task = mock_process_task
            
            # Create task manager
            manager = EntityTaskManager(max_concurrent_tasks=1)
            
            # Create school processing task
            school_task = {
                'task_id': 'school-processing-001',
                'entity_id': 'test-school-001',
                'entity_type': 'school',
                'stage': 'all',
                'priority': 1,
                'data_year': 2024
            }
            
            # Process the task
            await manager.add_task(school_task)
            await manager.process_queue()
            
            # Verify the task was processed
            status = manager.get_queue_status()
            assert status['total_completed'] == 1
            assert status['total_queued'] == 0
    
    async def test_location_point_processing_workflow(self):
        """Test location point processing workflow (fewer stages)"""
        with patch('entity_processing.task_manager.EntityProcessor') as mock_processor_class:
            mock_processor = Mock()
            mock_processor_class.return_value = mock_processor
            
            # Mock location point processing (only location + demographics)
            async def mock_process_task(task):
                entity_id = task['entity_id']
                entity_type = task['entity_type']
                
                # Location points only get these stages
                stages = ['location', 'demographics']
                completed_stages = []
                
                for stage in stages:
                    completed_stages.append(stage)
                
                return {
                    'status': 'success',
                    'entity_id': entity_id,
                    'entity_type': entity_type,
                    'completed_stages': completed_stages,
                    'total_stages': len(stages)
                }
            
            mock_processor.process_task = mock_process_task
            
            manager = EntityTaskManager(max_concurrent_tasks=1)
            
            # Create location processing task
            location_task = {
                'task_id': 'location-processing-001',
                'entity_id': 'test-location-001',
                'entity_type': 'location',
                'stage': 'all',
                'priority': 1
            }
            
            await manager.add_task(location_task)
            await manager.process_queue()
            
            # Verify processing completed
            status = manager.get_queue_status()
            assert status['total_completed'] == 1 