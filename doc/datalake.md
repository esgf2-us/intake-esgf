# Shared Group Cache

As you use `intake-esgf`, you build up a cache of all the datasets referenced in your research work. If you are working on a personal machine, then this cache helps *you* from redownloading files you have already used.

However, more often than not you do not work alone. You are part of a research group with a similar focus. The datasets that you use are possibly the same ones that others in your group use. If you work on a shared machine, then you can make your cache usable to the whole group. Your group can work collectively to build an ESGF data lake with the datasets used by everyone.

By default, `intake-esgf` will download data to a hidden folder in your home directory: `${HOME}/.esgf`. If you are going to be using `intake-esgf` on an institutional machine, you likely have a restrictive home directory quota which can fill up rather quickly with ESGF data. Disk space quotas for projects tend to be more permissive.

On institutional clusters or other shared machines, we recommend you change the location of the local cache to a path that is at least readable by every user on the machine and writable to everyone in your group. For example, on OLCF machines I am part of a project called `cli137`. By consulting the OLCF documentation, I found a group writable and world readable location that belongs to the project (`/lustre/orion/cli137/world-shared`) and created a directory there called `ESGF-Data`.

To change the local cache location, we need to interact with `intake-esgf` [configuration](configure):

```python
import intake_esgf
intake_esgf.conf.set(local_cache="/lustre/orion/cli137/world-shared/ESGF-data") # <-- set option for this session
intake_esgf.conf.save() # <-- make current options my default
```

Your path will look different, and you should first discuss this with the PI of the project associated with your account for permission to use this space in this way. Anyone who wants to participate and benefit from the group cache should run these python lines once.
