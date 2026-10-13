# cloud-bootstrap (temporary)

Byte-for-byte copies of the two files that
[cloud-customer-base-images-v2#156](https://github.com/laravel/cloud-customer-base-images-v2/pull/156)
(commit `2f0ff48a`) adds to the Python base image. They are here so this app behaves as it will once
that release reaches production. **Delete this folder and the Cloud build command once a base image
with #156 is live** (check: the "CPU limit" row still passes after removal).

Why: Cloud pods get a CPU quota and memory limit but no cpuset, so `os.cpu_count()`, `os.sysconf()`
and the stdlib pool defaults report the whole node (16 CPUs, ~124 GiB on a 2 vCPU / 4 GiB pod).
`laravel_cloud_bootstrap.py` caps them at the cgroup limits when `LARAVEL_CLOUD=1`; it is a no-op
locally.

How it is installed: the base image copies both files into the system site-packages. Here, every
environment's **build command** copies them into the user site-packages, where Cloud installs this
app's dependencies (`/var/www/.local/lib/python3.X/site-packages`). Python runs `.pth` files from
there at startup, so the hook loads in every interpreter (web, queue workers, `cpx cloud command:run`). The `if` makes it a no-op on branches without this folder, so it can stay set
before and after this folder exists:

```sh
if [ -d cloud-bootstrap ]; then mkdir -p "$(python -m site --user-site)" && cp cloud-bootstrap/laravel_cloud_bootstrap.py cloud-bootstrap/zz_laravel_cloud_bootstrap.pth "$(python -m site --user-site)/"; fi
```
