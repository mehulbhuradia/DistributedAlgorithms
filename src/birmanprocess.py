from abstractprocess import AbstractProcess, Message


class BirmanProcess(AbstractProcess):
    """
    Example implementation of a distributed process.
    This example sends and echo's 10 messages to another process.
    Only the algorithm() function needs to be implemented.
    The function send_message(message, to) can be used to send asynchronous messages to other processes.
    The variables first_cycle and num_echo are examples of variables custom to the EchoProcess algorithm.
    """
    first_cycle = True
    num_echo = 15
    
    async def algorithm(self):

        causual_order=[]
        # Only run in the beginning
        if self.first_cycle:
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
                # adding a radom delay to every message before it is sent but after updating the vector clock
                await self._random_delay()
                await self.send_message(msg, to)
            self.first_cycle = False


        # If we have a new message
        if self.buffer.has_messages():
            # Retrieve message
            msg: Message = self.buffer.get()

            print(f"Process {self.idx} received message from process {msg.sender} with timestamp: { msg.timestamp} ")
            for index,clockval in enumerate(msg.timestamp):
                
                # wait untile conditions are met
                if index == msg.sender:
                    if not clockval == self.vector_clock[index] + 1:
                        self.buffer.put(msg)
                        print(index)
                        print("Message from process {} is added to the message buffer.".format(msg.sender))
                        print(f"Message Timestamp: {msg.timestamp}, Vector Clock: {self.vector_clock}")
                        return
                else:
                    if not  clockval <= self.vector_clock[index]:
                        self.buffer.put(msg)
                        print("Message from process {} is added to the message buffer.".format(msg.sender))
                        print(f"Message Timestamp: {msg.timestamp}, Vector Clock: {self.vector_clock}")
                        return
            
            print(f"Delivered Message from process {msg.sender} to process {self.idx}")
            causual_order.append(msg)
            print(f"Message Timestamp: {msg.timestamp}, Vector Clock: {self.vector_clock}")
            for index,clockval in enumerate(msg.timestamp):
                self.vector_clock[index] = max(clockval,self.vector_clock[index])
            print("Updating process clock")
            print(f"Message Timestamp: {msg.timestamp}, New Vector Clock: {self.vector_clock}")           
        else:
            for idx,m in enumerate(causual_order):
                print(f"Message No.: {idx+1}, Sender: {msg.sender}, Timestamp: { msg.timestamp} ")
            self.running = False

        