from abstractprocess import AbstractProcess, Message


class BirmanProcess(AbstractProcess):
    """
    Example implementation of a distributed process.
    This example sends and echo's 10 messages to another process.
    Only the algorithm() function needs to be implemented.
    The function send_message(message, to) can be used to send asynchronous messages to other processes.
    The variables first_cycle and num_echo are examples of variables custom to the EchoProcess algorithm.
    """

    
    total_msg=10 # number of messages sent by other processes (num_msg*(num_processes-1))
    num_msg = 5
    causual_order=[]
    async def algorithm(self):
        # Only run in the beginning
        if self.num_msg!=0:
            # Compose message
            # Get first address we can find
            
            # Send message
            self.vector_clock[self.idx]+=1
            
            # timestamp of the message
            timestamp=self.vector_clock
            
            msg = Message("Hello world", self.idx, timestamp)
            
            print(f"Process {self.idx} broadcasting message with timestamp: {msg.timestamp}")
            
            #  broadcast to everyone
            for to in list(self.addresses.keys()):
                
                await self.send_message(msg, to)
            self.num_msg-=1

        
        # If we have a new message
        if self.buffer.has_messages():
            # adding a radom delay to every message before it is sent but after updating the vector clock
            await self._random_delay()
            # Retrieve message    
            msg: Message = self.buffer.get()

            print(f"Process {self.idx} received message from process {msg.sender} with timestamp: { msg.timestamp} ")
            for index,clockval in enumerate(msg.timestamp): 
                # wait untile conditions are met
                if index == msg.sender:
                    if not (clockval == (self.vector_clock[index] + 1)):
                        self.buffer.put(msg)
                        print(index)
                        print("Message from process {} is added to the message buffer.".format(msg.sender))
                        print(f"Message Timestamp: {msg.timestamp}, Vector Clock: {self.vector_clock}")
                        return
                else:
                    if not  (clockval <= self.vector_clock[index]):
                        self.buffer.put(msg)
                        print("Message from process {} is added to the message buffer.".format(msg.sender))
                        print(f"Message Timestamp: {msg.timestamp}, Vector Clock: {self.vector_clock}")
                        return
            
            print(f"Delivered Message from process {msg.sender} to process {self.idx}")
            temp_clock=self.vector_clock.copy()
            self.causual_order.append({"msg":msg,"clock":temp_clock})
            print(f"Message Timestamp: {msg.timestamp}, Vector Clock: {self.vector_clock}")
            for index,clockval in enumerate(msg.timestamp):
                self.vector_clock[index] = max(clockval,self.vector_clock[index])
            print("Updating process clock")
            print(f"Message Timestamp: {msg.timestamp}, New Vector Clock: {self.vector_clock}")           
        elif len(self.causual_order)==self.total_msg:
            for idx,m in enumerate(self.causual_order):
                print(f"Message No.: {idx+1}, Sender: {m['msg'].sender}, Timestamp: { m['msg'].timestamp}, Clock: {m['clock']}")
            self.running = False

        