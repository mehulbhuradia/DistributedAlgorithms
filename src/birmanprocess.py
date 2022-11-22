import asyncio

from abstractprocess import AbstractProcess, Message


class BirmanProcess(AbstractProcess):
    """
    Example implementation of a distributed process.
    This example sends and echo's 10 messages to another process.
    Only the algorithm() function needs to be implemented.
    The function send_message(message, to) can be used to send asynchronous messages to other processes.
    The variables first_cycle and num_echo are examples of variables custom to the EchoProcess algorithm.
    """

    total_msg = 2  # number of messages sent by other processes (num_broadcasts*(num_processes-1))
    num_broadcasts = 1  # Number of broadcasts made by each process
    broadcasts_sent = 0  # Counts the number of broadcasts a process has sent
    msg_sent = 0  # Counts the number of individual messages a process has sent
    causal_order = []  # Stores the order of received messages
    test_case: int = 0
    broadcast_tasks = set()

    # Adds delay to a single message before sending it
    async def send_delayed_message(self, msg, to, broadcasts_sent):
        await self.delay_individual_message(to, broadcasts_sent)
        await self.send_message(msg, to)
        self.msg_sent += 1

    # Adds a delay to a specific message within a broadcast
    async def delay_individual_message(self, to, broadcasts_sent):
        # TEST CASE 1:
        # Simulate a slow connection from P_0 -> P_N.
        # Expected result: P_N will delay messages received from its faster connections that happened after
        # the broadcast of P_0 (but were received before due to slow connection).
        if self.test_case == 1:
            self.num_broadcasts = 1
            self.total_msg = 2
            # Delay every message P_0 -> P_N
            if self.idx == 0 and to == list(self.addresses.keys())[-1]:
                await self._random_delay(10, 10)

        # TEST CASE 2:
        # P_0 broadcasts messages m1 -> m2.
        # P_N receives m2 before m1.
        # Expected result: P_N should delay m2 until m1 is delivered.
        if self.test_case == 2:
            self.num_broadcasts = 2
            self.total_msg = 4
            # Delay first message P_0 -> P_N only
            if self.idx == 0 and to == list(self.addresses.keys())[-1]:
                if broadcasts_sent == 0:
                    await self._random_delay(10, 10)

    # Adds a delay to all messages within a broadcast
    async def delay_entire_broadcast(self):
        # Add delay to P_1 to ensure that P_0 broadcasts first in test case 1
        if self.test_case == 1:
            if self.idx == 1:
                await self._random_delay(5, 5)

    async def broadcast(self):
        # SEND MSG
        if self.broadcasts_sent < self.num_broadcasts:
            await self.delay_entire_broadcast()
            self.vector_clock[self.idx] += 1
            timestamp = self.vector_clock.copy()
            msg = Message("Hello world", self.idx, timestamp)
            print(f"Process {self.idx} BROADCASTING message with timestamp: {msg.timestamp}")

            #  BROADCAST
            # Note: control of execution will immediately return to the process (needed for test case 2).
            # This means that any delay added to a single message will not affect subsequent messages.
            for to in list(self.addresses.keys()):
                # broadcasts_sent is passed to avoid concurrency issues
                broadcast_task = asyncio.create_task(self.send_delayed_message(msg, to, self.broadcasts_sent))
                self.broadcast_tasks.add(broadcast_task)
                broadcast_task.add_done_callback(self.broadcast_tasks.discard)

            # Concurrent broadcasts may increment broadcasts_sent simultaneously, use with caution
            self.broadcasts_sent += 1

    async def algorithm(self):
        # RECEIVE MSG:
        #   if delivery condition is true -> deliver
        #   else -> put back in buffer
        if self.buffer.has_messages():
            msg: Message = self.buffer.get()
            print(f"Process {self.idx} RECEIVED message from process {msg.sender} with timestamp: {msg.timestamp} ")
            deliverable = True

            # CHECK DELIVERY CONDITION
            for process_idx, timestamp_value in enumerate(msg.timestamp):
                if process_idx == msg.sender:
                    if not (timestamp_value == (self.vector_clock[process_idx] + 1)):
                        deliverable = False
                        self.buffer.put(msg)
                        print(F"Process {self.idx} DELAYED(1) message from process {msg.sender}")
                elif not (timestamp_value <= self.vector_clock[process_idx]):
                    deliverable = False
                    self.buffer.put(msg)
                    print(F"Process {self.idx} DELAYED(2) message from process {msg.sender}")

            # DELIVER
            if deliverable:
                print(F"Process {self.idx} DELIVERED message from process {msg.sender}")
                temp_clock = self.vector_clock.copy()
                self.causal_order.append({"msg": msg, "clock": temp_clock})
                for process_idx, timestamp_value in enumerate(msg.timestamp):
                    self.vector_clock[process_idx] = max(timestamp_value, self.vector_clock[process_idx])

        # EXIT CONDITION:
        # The process has sent and received #total_msg
        if len(self.causal_order) == self.total_msg and self.msg_sent == self.total_msg:
            for idx, m in enumerate(self.causal_order):
                print(
                    f"Message No.: {idx + 1}, Sender: {m['msg'].sender}, Timestamp: {m['msg'].timestamp}, Clock: {m['clock']}")
            self.running = False

        