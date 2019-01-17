# class for handling processes
import subprocess
import datetime


class Process:
    def __init__(self, master, cmd, buff_handle=None):
        self.master = master
        self.cmd = cmd
        self.buffHandle = buff_handle

        if cmd[0] == "magick":  # if it's an imagemagick command
            self.destFile = self.cmd[-1]  # path for destination file
            self.process = subprocess.Popen(self.cmd)
        else:
            self.process = subprocess.Popen(self.cmd, shell=True)

    # checks to see if a process has completed, returns itself and notifies it's buffer handler if it has
    def update(self):
        poll = self.process.poll()
        if poll is not None:  # checks for completion
            output, error = self.process.communicate()

            # notifies buffer handler of completion
            if self.buffHandle is not None:
                self.buffHandle.completed = True

            curDT = datetime.datetime.now()
            self.master.log_result((self.cmd, output, error, str(curDT)))  # logs the current date and time
            return self
