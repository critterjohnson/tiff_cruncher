# handles buffers assigned to a process
import os
from tiff_cruncher.process import Process


class BufferHandler:
    def __init__(self, master, process=None, pre=None, get=None, post=None, final=None):
        self.master = master  # the master in charge of this buffer handler
        self.process = process  # the process the buffer handler is attached to
        self.preProcess = None  # the process responsible for moving the image to the cache
        self.get = get  # the location the file is in before it is buffered
        self.post = post  # the location of the post-cache
        self.final = final  # the final location of the file after exiting the post-cache
        self.pre = pre  # the location the file should be in after it is buffered
        self.preCompleted = False  # True if pre-caching is complete
        self.completed = False  # true once the file this is responsible for has been moved to post

    @property
    def pre(self):
        return self._pre

    @pre.setter
    def pre(self, loc):
        self._pre = loc
        if self._pre is not None:
            cmd = ["copy", self.get, os.path.join(self.pre, os.path.split(self.get)[1])]  # move file command
            self.preProcess = Process(self.master, cmd)  # initializes the pre-process

    # returns True once the pre-caching process is complete
    def update(self):
        if self.preProcess.update() == self.preProcess:
            self.preCompleted = True
        return self.preCompleted

    def clear_pre(self):
        cmd = ["del", os.path.join(self.pre, os.path.split(self.get)[1])]
        prebuff_process = Process(self.master, cmd)
        while prebuff_process.update() is None:
            pass

    def clear_post(self):
        cmd = ["move", os.path.join(self.post, os.path.split(self.final)[1]), self.final]
        postbuff_process = Process(self.master, cmd)
        # waits until the post process is done
        while postbuff_process.update() is None:
            pass

    # copies its completed file over to the final location, clears self from pre-cache, returns itself when completed
    def finalize(self):
        if self.pre is not None:
            self.clear_pre()
        if self.post is not None:
            self.clear_post()
        return self
