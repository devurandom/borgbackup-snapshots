#!/usr/bin/python3.6

import sys, os, re, logging, atexit, json, argparse, pathlib
from logging import info, warn, debug
from os.path import realpath, dirname, join as pathjoin, ismount as is_mountpoint, isabs as is_absolute
from os import sep as pathsep, listdir
from subprocess import run, Popen as popen, PIPE
from time import time

import psutil


script_dir = dirname(realpath(__file__))

time_intervals = {
	"day": 60*60*24,
	"week": 60*60*24*7,
	"month": 60*60*24*7*4,
}

generic_subvolume_regex = re.compile(r'^.*-(\d+)$')

snapshotable_fstypes = ("btrfs",)


def closest(target, list):
	best = 0
	best_distance = target

	for current in list:
		distance = abs(target - current)
		if distance < best_distance:
			best = current
			best_distance = distance

	if best != 0:
		return best


def remove_prefix(string, prefix):
	return string[len(prefix):] if string.startswith(prefix) else string


def subvolume_from_mountpoint(btrfs, mountpoint):
	if not is_mountpoint(mountpoint):
		raise RuntimeError("{} is not a mountpoint".format(mountpoint))

	btrfs_subvolume_show = run(
		[btrfs, "subvolume", "show", mountpoint],
		stdout=PIPE,
		universal_newlines=True,
		check=True,
	)

	return btrfs_subvolume_show.stdout.splitlines()[0]


def subvolume_name_from_subvolume(subvolume):
	name = subvolume.replace("/", "_")
	if name == "_":
		name = "ROOT"
	return name


def snapshot(btrfs, mountpoint, snapshot_dir):
	info("Snapshotting {} into {} ...".format(mountpoint, snapshot_dir))

	run(
		[btrfs, "subvolume", "snapshot", "-r", mountpoint, snapshot_dir],
		check=True,
	)


def backup(name, config, snapshot_dir):
	info("Backing up {} into {} ...".format(config["mountpoint"], config["repository"]))
	if not snapshot_dir:
		warn("Mountpoint {} not snapshotted, doing live backup!".format(config["mountpoint"]))
		snapshot_dir = config["mountpoint"]

	command = [
		config["borg"],
		"--show-rc",
		"--show-version",
		"create",
		"--list",
		"--filter=E",
		"--stats",
		"--compression=lz4",
		"--one-file-system",
		"--exclude-caches",
	] + ["--exclude=" + v for v in config["excludes"]] + [
		"::{hostname}-{utcnow}",
		"."
	]
	if config["nice"]:
		command = ["ionice", "-c3", "nice", "-n10"] + command
	env = {**os.environ, "BORG_REPO": config["repository"]}
	result = run(command, cwd=snapshot_dir, env=env)
	if result.returncode != 0:
		warn("{} exited with return code {}, check the logs!".format(command, result.returncode))
	return result.returncode == 0


def prune_backups(name, config):
	info("Pruning backups of {} in {} ...".format(config["mountpoint"], config["repository"]))

	command = [
		config["borg"],
		"--show-rc",
		"--show-version",
		"prune",
		"--list",
		"--prefix={hostname}-",
		"--keep-daily=7",
		"--keep-weekly=4",
		"--keep-monthly=6",
	]
	if config["nice"]:
		command = ["ionice", "-c3", "nice", "-n10"] + command
	env = {**os.environ, "BORG_REPO": config["repository"]}
	run(command, env=env, check=True)


def prune_snapshots(btrfs, now, snapshot_dir, snapshots): # Assumes snapshot dirnames end in ...-TIMESTAMP
	info("Pruning snapshots in {} ...".format(snapshot_dir))

	daily=7
	weekly=4
	monthly=6

	wanted_snapshots = []
	for _ in range(daily):
		wanted_snapshots += ["day"]
	for _ in range(weekly):
		wanted_snapshots += ["week"]
	for _ in range(monthly):
		wanted_snapshots += ["month"]

	actual_timestamps = []
	for s in snapshots:
		t = int(generic_subvolume_regex.sub(r'\1', s))
		debug("Found snapshot {} at timestamp {}".format(s, t))
		actual_timestamps += [t]

	last_kept_timestamp = now
	keep_timestamps = []
	for next_snapshot in wanted_snapshots:
		target_timestamp = last_kept_timestamp - time_intervals[next_snapshot]
		closest_timestamp = closest(target_timestamp, actual_timestamps)
		if not closest_timestamp:
			break
		debug("Found {} as candidate for {} in {}".format(closest_timestamp, target_timestamp, actual_timestamps))
		actual_timestamps.remove(closest_timestamp)
		keep_timestamps += [closest_timestamp]

	for s in snapshots:
		t = int(generic_subvolume_regex.sub(r'\1', s))
		if t not in keep_timestamps:
			info("Pruning snapshot {} at timestamp {} ...".format(s, t))
			command = [btrfs, "subvolume", "delete", pathjoin(snapshot_dir, s)]
			run(command, check=True)


def filesystem_type(mountpoint):
	partitions = list(filter(lambda partition: partition.mountpoint == mountpoint, psutil.disk_partitions()))
	if not len(partitions) == 1:
		raise RuntimeError("Found more than one ({}) matching partitions for mountpoint {}".format(len(partitions), mountpoint))
	return partitions[0].fstype


if __name__ == '__main__':
	arg_parser = argparse.ArgumentParser(description='Create BTRFS snapshots and backup them using Borg')
	arg_parser.add_argument('--log-level', default='info', choices=['critical', 'error', 'warning', 'info', 'debug'])
	arg_parser.add_argument('--backup-only')
	arg_parser.add_argument('--snapshot-dir', default=pathjoin(pathsep, "@snapshots"))
	arg_parser.add_argument('--with-borg', default="{}/borg".format(script_dir))
	arg_parser.add_argument('--with-btrfs', default="/usr/bin/btrfs")
	arg_parser.add_argument('--nice', type=bool)
	arg_parser.add_argument('config_file', type=argparse.FileType('r'))
	args = arg_parser.parse_args()

	debug("Running Python: {}".format(sys.version))

	if args.backup_only:
		args.backup_only = [item for item in args.backup_only.split(',')]

	logging.basicConfig(level=logging.getLevelName(args.log_level.upper()))
	atexit.register(logging.shutdown)

	backup_dir = dirname(realpath(args.config_file.name))
	debug("Backing up into {}".format(backup_dir))

	snapshot_dir = args.snapshot_dir
	if not is_mountpoint(snapshot_dir):
		raise RuntimeError("It is advised to make {} a mountpoint".format(snapshot_dir))

	debug("Loading config from {}".format(args.config_file.name))
	config = json.loads(args.config_file.read())
	args.config_file.close()

	now = int(time())

	messages = []

	pathlib.Path(snapshot_dir).mkdir(parents=True, exist_ok=True)

	# Snapshot now, as the backup might take a long time and the different mountpoints might get out of sync
	for name, backup_config in config.items():
		if args.backup_only and name not in args.backup_only:
			continue

		if not "excludes" in backup_config:
			backup_config["excludes"] = []
		if not is_absolute(backup_config["repository"]):
			backup_config["repository"] = realpath(pathjoin(backup_dir, backup_config["repository"]))

		backup_config["fstype"] = filesystem_type(backup_config["mountpoint"])
		backup_config["borg"] = args.with_borg
		backup_config["nice"] = args.nice

		if backup_config["fstype"] in snapshotable_fstypes:
			backup_config["subvolume"] = subvolume_from_mountpoint(args.with_btrfs, backup_config["mountpoint"])
			backup_config["subvolume_name"] = subvolume_name_from_subvolume(backup_config["subvolume"])
			backup_config["snapshot"] = pathjoin(snapshot_dir, "{}-{}".format(backup_config["subvolume_name"], now))
			snapshot(args.with_btrfs, backup_config["mountpoint"], backup_config["snapshot"])
		else:
			m = "Filesystem type {} of mountpoint {} not supported, unable to create snapshot!".format(backup_config["fstype"], backup_config["mountpoint"])
			messages += [m]
			warn(m)

	for name, backup_config in config.items():
		if args.backup_only and name not in args.backup_only:
			continue

		snapshot = None
		if "snapshot" in backup_config:
			snapshot = backup_config["snapshot"]
		if not backup(name, backup_config, snapshot):
			m = "Backup of {} exited with non-zero exit code, check the logs!".format(name)
			messages += [m]
			warn(m)

		prune_backups(name, backup_config)

		if backup_config["fstype"] in snapshotable_fstypes:
			subvolume_regex = re.compile(r'^' + backup_config["subvolume_name"] + r'-(\d+)$')
			snapshots = list(filter(lambda filename: subvolume_regex.match(filename), listdir(snapshot_dir)))
			prune_snapshots(args.with_btrfs, now, snapshot_dir, snapshots)

	info("Repeating all warnings:")
	for m in messages:
		warn(m)
