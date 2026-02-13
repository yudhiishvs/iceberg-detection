"""
Event queue for discrete event simulation.

Manages simulation time and schedules future events using a priority queue.
Events are processed in timestamp order.
"""

import heapq
from typing import Callable, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum


class EventType(Enum):
    """Types of events that can be scheduled"""
    ORDER_SUBMISSION = "ORDER_SUBMISSION"
    ORDER_CANCELLATION = "ORDER_CANCELLATION"
    ICEBERG_REFILL = "ICEBERG_REFILL"
    AGENT_ACTION = "AGENT_ACTION"
    SNAPSHOT = "SNAPSHOT"
    END_SIMULATION = "END_SIMULATION"


@dataclass(order=True)
class Event:
    """
    A scheduled event in the simulation.
    
    Events are ordered by timestamp, then by sequence number
    to break ties deterministically.
    """
    timestamp: float
    sequence: int = field(compare=True)
    event_type: EventType = field(compare=False)
    callback: Callable = field(compare=False)
    data: Any = field(default=None, compare=False)
    
    def execute(self):
        """Execute this event's callback"""
        if self.data is not None:
            return self.callback(self.data)
        else:
            return self.callback()


class EventQueue:
    """
    Priority queue for discrete event simulation.
    
    Maintains events sorted by timestamp and processes them in order.
    Supports scheduling future events and querying simulation state.
    """
    
    def __init__(self, start_time: float = 0.0):
        """
        Initialize the event queue.
        
        Args:
            start_time: Simulation start time
        """
        self._heap: List[Event] = []
        self._current_time = start_time
        self._sequence_counter = 0
        self._processed_count = 0
    
    @property
    def current_time(self) -> float:
        """Current simulation time"""
        return self._current_time
    
    @property
    def is_empty(self) -> bool:
        """Check if queue has any pending events"""
        return len(self._heap) == 0
    
    @property
    def size(self) -> int:
        """Number of pending events"""
        return len(self._heap)
    
    @property
    def processed_count(self) -> int:
        """Total number of events processed"""
        return self._processed_count
    
    def schedule(
        self,
        timestamp: float,
        event_type: EventType,
        callback: Callable,
        data: Any = None
    ) -> Event:
        """
        Schedule a future event.
        
        Args:
            timestamp: When to execute the event
            event_type: Type of event
            callback: Function to call when event executes
            data: Optional data to pass to callback
            
        Returns:
            The scheduled Event object
        """
        if timestamp < self._current_time:
            raise ValueError(
                f"Cannot schedule event in the past: {timestamp} < {self._current_time}"
            )
        
        event = Event(
            timestamp=timestamp,
            sequence=self._sequence_counter,
            event_type=event_type,
            callback=callback,
            data=data
        )
        
        heapq.heappush(self._heap, event)
        self._sequence_counter += 1
        
        return event
    
    def schedule_after(
        self,
        delay: float,
        event_type: EventType,
        callback: Callable,
        data: Any = None
    ) -> Event:
        """
        Schedule an event after a delay from current time.
        
        Args:
            delay: Time to wait before executing
            event_type: Type of event
            callback: Function to call when event executes
            data: Optional data to pass to callback
            
        Returns:
            The scheduled Event object
        """
        return self.schedule(
            timestamp=self._current_time + delay,
            event_type=event_type,
            callback=callback,
            data=data
        )
    
    def peek_next(self) -> Optional[Event]:
        """
        Look at the next event without removing it.
        
        Returns:
            Next event to be processed, or None if queue is empty
        """
        return self._heap[0] if self._heap else None
    
    def next_event_time(self) -> Optional[float]:
        """Get timestamp of next event"""
        next_event = self.peek_next()
        return next_event.timestamp if next_event else None
    
    def pop_next(self) -> Optional[Event]:
        """
        Remove and return the next event.
        
        Returns:
            Next event, or None if queue is empty
        """
        if not self._heap:
            return None
        
        event = heapq.heappop(self._heap)
        self._current_time = event.timestamp
        self._processed_count += 1
        
        return event
    
    def process_next(self) -> Tuple[Optional[Event], Any]:
        """
        Pop next event, execute it, and return result.
        
        Returns:
            Tuple of (event, callback_result)
        """
        event = self.pop_next()
        if event is None:
            return None, None
        
        result = event.execute()
        return event, result
    
    def process_until(self, end_time: float) -> int:
        """
        Process all events up to a given time.
        
        Args:
            end_time: Time to process until
            
        Returns:
            Number of events processed
        """
        count = 0
        while not self.is_empty:
            next_time = self.next_event_time()
            if next_time is None or next_time > end_time:
                break
            
            self.process_next()
            count += 1
        
        # Update current time even if no events remain
        if self.is_empty or end_time > self._current_time:
            self._current_time = end_time
        
        return count
    
    def process_all(self) -> int:
        """
        Process all pending events.
        
        Returns:
            Number of events processed
        """
        count = 0
        while not self.is_empty:
            self.process_next()
            count += 1
        
        return count
    
    def clear(self):
        """Remove all pending events"""
        self._heap.clear()
    
    def get_pending_events_by_type(self, event_type: EventType) -> List[Event]:
        """
        Get all pending events of a specific type.
        
        Args:
            event_type: Type to filter by
            
        Returns:
            List of matching events
        """
        return [e for e in self._heap if e.event_type == event_type]
    
    def cancel_events_by_type(self, event_type: EventType) -> int:
        """
        Remove all pending events of a specific type.
        
        Args:
            event_type: Type to remove
            
        Returns:
            Number of events cancelled
        """
        original_size = len(self._heap)
        self._heap = [e for e in self._heap if e.event_type != event_type]
        heapq.heapify(self._heap)
        
        return original_size - len(self._heap)
    
    def __repr__(self):
        return (
            f"EventQueue(time={self._current_time:.2f}, "
            f"pending={self.size}, processed={self._processed_count})"
        )
    
    def __len__(self):
        return self.size


class PeriodicEvent:
    """
    Helper class for scheduling recurring events.
    
    Automatically reschedules itself after each execution.
    """
    
    def __init__(
        self,
        event_queue: EventQueue,
        interval: float,
        event_type: EventType,
        callback: Callable,
        start_time: Optional[float] = None,
        max_occurrences: Optional[int] = None
    ):
        """
        Create a periodic event.
        
        Args:
            event_queue: Queue to schedule events on
            interval: Time between occurrences
            event_type: Type of event
            callback: Function to call
            start_time: When to start (None = current time + interval)
            max_occurrences: Stop after this many occurrences (None = infinite)
        """
        self.event_queue = event_queue
        self.interval = interval
        self.event_type = event_type
        self.callback = callback
        self.max_occurrences = max_occurrences
        self.occurrence_count = 0
        self.active = True
        
        # Schedule first occurrence
        if start_time is None:
            start_time = event_queue.current_time + interval
        
        self._schedule_next(start_time)
    
    def _wrapped_callback(self):
        """Wrapper that calls the callback and reschedules"""
        if not self.active:
            return
        
        # Execute user callback
        result = self.callback()
        
        self.occurrence_count += 1
        
        # Check if we should continue
        if (self.max_occurrences is None or 
            self.occurrence_count < self.max_occurrences):
            # Schedule next occurrence
            next_time = self.event_queue.current_time + self.interval
            self._schedule_next(next_time)
        else:
            self.active = False
        
        return result
    
    def _schedule_next(self, timestamp: float):
        """Schedule the next occurrence"""
        self.event_queue.schedule(
            timestamp=timestamp,
            event_type=self.event_type,
            callback=self._wrapped_callback
        )
    
    def cancel(self):
        """Stop this periodic event"""
        self.active = False