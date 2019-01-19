# master process for the tiff cruncher, manages all sub-processes
import timeit
import os
from tiff_cruncher.process import Process
from tiff_cruncher.bufffer_handler import BufferHandler
import sys


class Master:
    # constructor
    def __init__(self,
                 path,  # path of the directory to run on
                 dest,  # destination folder
                 total_processes=10,  # maximum number of concurrent running processes
                 pre_buff_location=None,  # location of the pre buffer
                 post_buff_location=None,  # location of the post buffer
                 pre_buff_size=None,  # size of the pre buffer
                 post_buff_size=None,  # size of the post buffer
                 log_file=None):  # location of the log file

        self.fileTree = path
        self.path = path
        self.dest = dest
        self.totalProcesses = total_processes
        self.preBuffLocation = pre_buff_location
        self.postBuffLocation = post_buff_location
        self.preBuffSize = pre_buff_size
        self.postBuffSize = post_buff_size
        self.logFile = log_file

        self.processes = set()
        self.numProcesses = 0
        self.bufferHandlers = set()
        self.curBuffHandler = None
        self.commands = []
        if self.logFile is not None:
            self.log = []
        else:
            self.log = None

    @property
    def fileTree(self):
        return self._fileTree

    @fileTree.setter
    def fileTree(self, path):
        self._fileTree = get_file_tree(path)

    # process for running the master program
    def run(self):
        # create commands based off files
        for file in self.fileTree:
            final = os.path.join(self.dest, file.split(self.path)[-1][1:])  # gets the correct file path
            os.makedirs(os.path.split(final)[0], exist_ok=True)  # creates the directory structure
            final = final[0:-4] + ".jpg"  # gets the correct file name
            self.commands.append(["magick", "convert", file, "-compress", "jpeg", "-quality", "90", final])

        # if there's no need to buffer, runs without any buffers
        if self.preBuffLocation is None and self.postBuffLocation is None:
            for command in self.commands:
                # updates all processes until it can continue
                while self.numProcesses == self.totalProcesses:
                    self.update_processes()
                # creates a Process object to run the process
                self.processes.add(Process(self, command))
                self.numProcesses = len(self.processes)

        # if there are buffers
        else:
            for command in self.commands:
                # runs the following code only if it's a magic command
                if command[0] == "magick":
                    # if necessary, creates a new buffer handler
                    if self.curBuffHandler is None:
                        # creates a new buffer handler and changes the command to accomadate
                        self.curBuffHandler, command = self.create_buff_handler(command)
                    # if necessary, waits for the current buffer handler to complete
                    if self.preBuffLocation is not None:
                        while not self.curBuffHandler.preCompleted:
                            self.curBuffHandler.update()
                    # if necessary, clears the post cache if it is too large
                    if self.postBuffLocation is not None:
                        if self.postBuffSize is not None:
                            size = get_dir_size(self.postBuffLocation)
                            if size * 0.000001 >= self.postBuffSize:
                                self.clear_post()
                    # if necessary, clears out the pre-cache if it is too large
                    if self.preBuffLocation is not None:
                        if self.postBuffSize is not None:
                            size = get_dir_size(self.preBuffLocation)
                            if size * 0.000001 >= self.preBuffSize:
                                self.clear_pre()
                    # updates all processes until it can continue
                    while self.numProcesses == self.totalProcesses:
                        self.update_processes()
                        self.numProcesses = len(self.processes)
                    # attaches a process to the current buffer handler
                    process = Process(self, command)
                    self.processes.add(process)
                    self.numProcesses = len(self.processes)
                    self.curBuffHandler.process = process
                    self.bufferHandlers.add(self.curBuffHandler)
                    self.curBuffHandler = None  # prepares to create a new current buffer handler
                # if it's some other kind of command, just spawn a new process
                else:
                    # updates all processes until it can continue
                    while self.numProcesses == self.totalProcesses:
                        self.update_processes()
                    # creates a Process object to run the process
                    self.processes.add(Process(self, command))
                    self.numProcesses = len(self.processes)

        # updates all processes until they have completed
        while not self.numProcesses == 0:
            self.update_processes()
        # clears the buffers
        if self.postBuffLocation is not None or self.preBuffLocation is not None:
            self.clear_buff()
        # logs everything
        if self.logFile is not None:
            with open(self.logFile, "w") as log:
                for event in self.log:
                    log.write(str(event) + "\n")

    # checks all sub-processes for completion and removes completed sub-processes
    def update_processes(self):
        remove = set()
        for process in self.processes:
            remove.add(process.update())
        self.processes.difference_update(remove)
        self.numProcesses = len(self.processes)

    # creates a buffer handler with the proper command, cache, and final locations
    def create_buff_handler(self, command):
        get = command[2]
        final = None
        # gets everything required for a pre-cache
        if self.preBuffLocation is not None:
            command[2] = os.path.join(self.preBuffLocation, os.path.split(get)[1])
        # gets everything required for a post-cache
        if self.postBuffLocation is not None:
            final = command[7]
            command[7] = os.path.join(self.postBuffLocation, os.path.split(final)[1])
        # creates the buffer handler
        handler = BufferHandler(self, pre=self.preBuffLocation, get=get, post=self.postBuffLocation, final=final)
        return handler, command

    # clears the pre-cache
    def clear_pre(self):
        for handler in self.bufferHandlers:
            if handler.preCompleted:
                handler.clear_pre()

    # clears the post-cache and itself from the buffer handlers
    def clear_post(self):
        remove = set()
        for handler in self.bufferHandlers:
            handler.clear_post()
            remove.add(handler)
        self.bufferHandlers.difference_update(remove)

    # clears all completed files from all buffers
    def clear_buff(self):
        if self.preBuffLocation is not None:
            self.clear_pre()
        if self.postBuffLocation is not None:
            self.clear_post()

    def log_result(self, to_log):
        if self.logFile is not None:
            self.log.append(to_log)


# gets the size of a directory in bytes
def get_dir_size(path):
    size = 0
    for path, dirs, files in os.walk(path):
        for f in files:
            cur = os.path.join(path, f)
            size += os.path.getsize(cur)
    return size


# returns a full list of all the files in a directory tree
def get_file_tree(path):
    list = []
    for root, dirs, files in os.walk(path):
        for name in files:
            list.append(os.path.join(root, name))
    return list


# main method
def main():
    # argument list
    args = {
        "path": {
            "value": None,
            "required": True
        },
        "dest": {
            "value": None,
            "required": True
        },
        "total_processes": {
            "value": None,
            "required": False
        },
        "pre_buff_location": {
            "value": None,
            "required": False
        },
        "post_buff_location": {
            "value": None,
            "required": False
        },
        "pre_buff_size": {
            "value": None,
            "required": False
        },
        "post_buff_size": {
            "value": None,
            "required": False
        },
        "log_file": {
            "value": None,
            "required": False
        }
    }

    # arguments passed from console
    arguments = sys.argv[1:]

    # changes any None passes to none
    for i in range(len(arguments)):
        if arguments[i] == "None" or arguments[i] == "none":
            arguments[i] = None

    # gets all arguments and assigns them the correct value
    for i in range(len(arguments)):
        if arguments[i] == "-p":  # -p flags path
            args["path"]["value"] = arguments[i + 1]
        elif arguments[i] == "-dest":  # -dest flags destination folder
            args["dest"]["value"] = arguments[i+1]
        elif arguments[i] == "-tp":  # -tp flags total processes
            args["total_processes"]["value"] = int(arguments[i+1])
        elif arguments[i] == "-preloc":  # -preloc flags pre buffer location
            args["pre_buff_location"]["value"] = arguments[i + 1]
        elif arguments[i] == "-postloc":  # -postloc flags post buffer location
            args["post_buff_location"]["value"] = arguments[i + 1]
        elif arguments[i] == "-presize":  # -presize flags pre buffer size
            args["pre_buff_size"]["value"] = int(arguments[i + 1])
        elif arguments[i] == "-postsize":  # -postsize flags post buffer size
            args["post_buff_size"]["value"] = int(arguments[i + 1])
        elif arguments[i] == "-log":  # -log flags log file
            args["log_file"]["value"] = arguments[i + 1]

    # ensures that all required arguments have been passed
    for key, val in args.items():
        if val["required"] and val["value"] is None:
            raise ValueError(key + " is required.")

    master = Master(path=args["path"]["value"],
                    dest=args["dest"]["value"],
                    total_processes=args["total_processes"]["value"],
                    pre_buff_location=args["pre_buff_location"]["value"],
                    post_buff_location=args["post_buff_location"]["value"],
                    pre_buff_size=args["pre_buff_size"]["value"],
                    post_buff_size=args["post_buff_size"]["value"],
                    log_file=args["log_file"]["value"])
    master.run()


if __name__ == "__main__":
    print(timeit.timeit(main, number=1))

# TODO - go over code and try to make it look pretty
