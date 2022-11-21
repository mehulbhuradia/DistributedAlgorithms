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


    total_msg=2 # number of messages sent by other processes (num_msg*(num_processes-1))
    num_msg = 1  # Number of broadcasts made by each process
    msg_sent = 0
    msg_log: dict = {}
    causal_order=[]
    test_case: int = 0
    broadcast_tasks = set()

    async def broadcast_message(self, msg, to, msg_sent):
        await self.chaos_individual_message(to, msg_sent)
        await self.send_message(msg, to)
        self.msg_log[to] = self.msg_log.get(to, 0) + 1

    async def chaos_individual_message(self, to, msg_sent):
        # TEST CASE 1:
        # Simulate a slow connection from P_0 -> P_N.
        # Num broadcasts per process: 2
        # Expected result: P_N will delay messages received from its faster connections that happened after
        # the broadcast of P_0 (but were received before due to slow connection).
        if self.test_case == 1:
            self.num_msg = 1
            self.total_msg = 2
            if self.idx == 0 and to == list(self.addresses.keys())[-1]:
                await self._random_delay(10, 10)

        # TEST CASE 2:
        # P_0 broadcasts messages m1 -> m2.
        # P_N receives m2 before m1.
        # Expected result: P_N should delay m2 until m1 is delivered.
        if self.test_case == 2:
            self.num_msg = 2
            self.total_msg = 4
            if self.idx == 0 and to == list(self.addresses.keys())[-1]:
                # Delay first message only
                if msg_sent == 0:
                    await self._random_delay(10, 10)

    async def chaos_entire_broadcast(self):
        if self.test_case == 1:
            if self.idx == 1:
                await self._random_delay(5, 5)

    async def broadcast(self):
        # SEND MSG
        if self.msg_sent < self.num_msg:
            await self.chaos_entire_broadcast()
            self.vector_clock[self.idx] += 1
            timestamp = self.vector_clock.copy()
            msg = Message("Hello world", self.idx, timestamp)
            print(f"Process {self.idx} BROADCASTING message with timestamp: {msg.timestamp}")

            #  BROADCAST
            for to in list(self.addresses.keys()):
                broadcast_task = asyncio.create_task(self.broadcast_message(msg, to, self.msg_sent))
                self.broadcast_tasks.add(broadcast_task)
                broadcast_task.add_done_callback(self.broadcast_tasks.discard)

            self.msg_sent += 1

    async def algorithm(self):
        # RECEIVE MSG:
        #   if delivery condition is true -> deliver
        #   else -> put back in buffer
        if self.buffer.has_messages():
            # adding a radom delay to every message before it is sent but after updating the vector clock
            # await self._random_delay()
            # Receive message
            msg: Message = self.buffer.get()

            print(f"Process {self.idx} RECEIVED message from process {msg.sender} with timestamp: {msg.timestamp} ")
            deliverable = True
            for index, clockval in enumerate(msg.timestamp):
                #CHECK DELIVERY CONDITION
                if index == msg.sender:
                    if not (clockval == (self.vector_clock[index] + 1)):
                        deliverable = False
                        self.buffer.put(msg)
                        print(F"Process {self.idx} DELAYED(1) message from process {msg.sender}")
                        # print(f"Message Timestamp: {msg.timestamp}, Vector Clock: {self.vector_clock}")
                elif not (clockval <= self.vector_clock[index]):
                    deliverable = False
                    self.buffer.put(msg)
                    print(F"Process {self.idx} DELAYED(2) message from process {msg.sender}")
                    # print(f"Message Timestamp: {msg.timestamp}, Vector Clock: {self.vector_clock}")
            # DELIVER
            if deliverable:
                print(F"Process {self.idx} DELIVERED message from process {msg.sender}")
                temp_clock = self.vector_clock.copy()
                self.causal_order.append({"msg": msg, "clock": temp_clock})
                # print(f"Message Timestamp: {msg.timestamp}, Vector Clock: {self.vector_clock}")
                for index, clockval in enumerate(msg.timestamp):
                    self.vector_clock[index] = max(clockval, self.vector_clock[index])
                # print("Updating process clock")
                # print(f"Message Timestamp: {msg.timestamp}, New Vector Clock: {self.vector_clock}")

        # EXIT CONDITION
        if len(self.causal_order) == self.total_msg and sum(self.msg_log.values()) == self.total_msg:
            for idx, m in enumerate(self.causal_order):
                print(
                    f"Message No.: {idx + 1}, Sender: {m['msg'].sender}, Timestamp: {m['msg'].timestamp}, Clock: {m['clock']}")
            self.running = False

        