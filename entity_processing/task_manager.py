"""
Entity Task Manager

Manages task queues and distribution for entity processing.
Provides queue-based processing with different priorities and workload types.
"""

import asyncio
from flask import current_app
from firebase_admin import firestore
from datetime import datetime
from .processor import EntityProcessor
from .utils import validate_entity, success_response, error_response


class EntityTaskManager:
    def __init__(self):
        self.running = False
        self.tasks = {
            'high_priority': asyncio.Queue(),
            'normal_priority': asyncio.Queue(),
            'low_priority': asyncio.Queue(),
            'bulk_processing': asyncio.Queue()
        }
        self.workers = {}
        self.max_concurrent_tasks = 3
        
    async def start(self):
        """Start the task manager"""
        if self.running:
            return
        
        self.running = True
        current_app.logger.info("Starting Entity Task Manager")
        
        # Start worker tasks for each queue
        for queue_name in self.tasks:
            worker_count = 1 if queue_name == 'bulk_processing' else 2
            self.workers[queue_name] = []
            
            for i in range(worker_count):
                worker = asyncio.create_task(
                    self._worker(queue_name, i)
                )
                self.workers[queue_name].append(worker)
    
    async def stop(self):
        """Stop the task manager"""
        if not self.running:
            return
        
        self.running = False
        current_app.logger.info("Stopping Entity Task Manager")
        
        # Cancel all workers
        for queue_workers in self.workers.values():
            for worker in queue_workers:
                worker.cancel()
        
        # Clear queues
        for queue in self.tasks.values():
            while not queue.empty():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
    
    async def add_processing_task(self, entity_id, entity_type='school', data_year=None, priority='normal'):
        """Add a processing task to the queue"""
        try:
            # Validate entity
            validation_result = validate_entity(entity_id, entity_type)
            if not validation_result['valid']:
                return error_response(validation_result['error'])
            
            task_data = {
                'type': 'process_entity',
                'entity_id': entity_id,
                'entity_type': entity_type,
                'data_year': data_year,
                'created_at': datetime.utcnow(),
                'status': 'queued'
            }
            
            # Determine queue based on priority
            queue_name = f"{priority}_priority" if priority in ['high', 'normal', 'low'] else 'normal_priority'
            
            if queue_name not in self.tasks:
                queue_name = 'normal_priority'
            
            await self.tasks[queue_name].put(task_data)
            
            current_app.logger.info(f"Added {entity_type} {entity_id} to {queue_name} queue")
            
            return success_response({
                'entity_id': entity_id,
                'entity_type': entity_type,
                'queue': queue_name,
                'queued_at': task_data['created_at']
            }, f"Task queued in {queue_name}")
            
        except Exception as e:
            current_app.logger.error(f"Error adding task: {str(e)}")
            return error_response(str(e))
    
    async def add_bulk_processing_task(self, entity_list, entity_type='school', data_year=None):
        """Add a bulk processing task to the queue"""
        try:
            task_data = {
                'type': 'bulk_process',
                'entity_list': entity_list,
                'entity_type': entity_type,
                'data_year': data_year,
                'created_at': datetime.utcnow(),
                'status': 'queued'
            }
            
            await self.tasks['bulk_processing'].put(task_data)
            
            current_app.logger.info(f"Added bulk processing task for {len(entity_list)} {entity_type}s")
            
            return success_response({
                'entity_count': len(entity_list),
                'entity_type': entity_type,
                'queue': 'bulk_processing',
                'queued_at': task_data['created_at']
            }, f"Bulk task queued for {len(entity_list)} entities")
            
        except Exception as e:
            current_app.logger.error(f"Error adding bulk task: {str(e)}")
            return error_response(str(e))
    
    async def _worker(self, queue_name, worker_id):
        """Worker function for processing tasks from a queue"""
        current_app.logger.info(f"Starting worker {worker_id} for queue {queue_name}")
        
        while self.running:
            try:
                # Get task from queue with timeout
                task_data = await asyncio.wait_for(
                    self.tasks[queue_name].get(),
                    timeout=5.0
                )
                
                current_app.logger.info(f"Worker {worker_id} processing task: {task_data['type']}")
                
                # Process the task
                if task_data['type'] == 'process_entity':
                    await self._process_single_entity(task_data, worker_id)
                elif task_data['type'] == 'bulk_process':
                    await self._process_bulk_entities(task_data, worker_id)
                
                # Mark task as done
                self.tasks[queue_name].task_done()
                
            except asyncio.TimeoutError:
                # No tasks available, continue loop
                continue
            except Exception as e:
                current_app.logger.error(f"Worker {worker_id} error: {str(e)}")
                # Mark task as done even on error to prevent queue blocking
                try:
                    self.tasks[queue_name].task_done()
                except:
                    pass
        
        current_app.logger.info(f"Worker {worker_id} for queue {queue_name} stopped")
    
    async def _process_single_entity(self, task_data, worker_id):
        """Process a single entity"""
        try:
            entity_id = task_data['entity_id']
            entity_type = task_data['entity_type']
            data_year = task_data['data_year']
            
            current_app.logger.info(f"Worker {worker_id} processing {entity_type} {entity_id}")
            
            # Create processor and run
            processor = EntityProcessor(entity_id, entity_type, data_year)
            success, error_msg = await processor.process()
            
            if success:
                current_app.logger.info(f"Worker {worker_id} completed {entity_type} {entity_id}")
            else:
                current_app.logger.error(f"Worker {worker_id} failed {entity_type} {entity_id}: {error_msg}")
            
        except Exception as e:
            current_app.logger.error(f"Worker {worker_id} processing error: {str(e)}")
    
    async def _process_bulk_entities(self, task_data, worker_id):
        """Process multiple entities in bulk"""
        try:
            entity_list = task_data['entity_list']
            entity_type = task_data['entity_type']
            data_year = task_data['data_year']
            
            current_app.logger.info(f"Worker {worker_id} bulk processing {len(entity_list)} {entity_type}s")
            
            results = {
                'processed': 0,
                'failed': 0,
                'skipped': 0
            }
            
            for entity_data in entity_list:
                try:
                    entity_id = entity_data.get('entity_id') if isinstance(entity_data, dict) else entity_data
                    
                    # Validate entity
                    validation_result = validate_entity(entity_id, entity_type)
                    if not validation_result['valid']:
                        results['skipped'] += 1
                        continue
                    
                    # Process entity
                    processor = EntityProcessor(entity_id, entity_type, data_year)
                    success, error_msg = await processor.process()
                    
                    if success:
                        results['processed'] += 1
                    else:
                        results['failed'] += 1
                        
                except Exception as e:
                    current_app.logger.error(f"Bulk processing error for entity {entity_id}: {str(e)}")
                    results['failed'] += 1
            
            current_app.logger.info(f"Worker {worker_id} bulk processing completed: {results}")
            
        except Exception as e:
            current_app.logger.error(f"Worker {worker_id} bulk processing error: {str(e)}")
    
    def get_queue_status(self):
        """Get status of all queues"""
        try:
            status = {}
            for queue_name, queue in self.tasks.items():
                status[queue_name] = {
                    'size': queue.qsize(),
                    'workers': len(self.workers.get(queue_name, []))
                }
            
            return {
                'running': self.running,
                'queues': status,
                'total_pending': sum(q.qsize() for q in self.tasks.values())
            }
        except Exception as e:
            current_app.logger.error(f"Error getting queue status: {str(e)}")
            return {'error': str(e)}


# Global task manager instance
task_manager = EntityTaskManager() 