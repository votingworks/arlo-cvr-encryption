# Inspiration: https://github.com/honnibal/spacy-ray/pull/1/files#diff-7ede881ddc3e8456b320afb958362b2aR12-R45
from asyncio import Event
from dataclasses import dataclass
from typing import Dict

import ray
from ray.actor import ActorHandle
from tqdm import tqdm


@dataclass
class ProgressBarState:
    """
    Internal state of the ProgressBar.
    """

    counter: int
    total: int

    def update_completed(self, delta_num_items_completed: int) -> None:
        self.counter += delta_num_items_completed

    def update_total(self, delta_total: int) -> None:
        self.total += delta_total


@ray.remote
class ProgressBarActor:
    """
    This is the Ray "actor" that can be called from anywhere to update our progress.
    You'll be using the `update`* methods. Don't instantiate this class yourself. Instead,
    it's something that you'll get from a `ProgressBar`.
    """

    state: Dict[str, ProgressBarState]
    event: Event

    def __init__(self, totals: Dict[str, int]) -> None:
        self.state = {key: ProgressBarState(0, totals[key]) for key in totals.keys()}
        self.event = Event()

    def update_completed(self, key: str, delta_num_items_completed: int) -> None:
        """
        Updates the ProgressBar with the incremental number of items that
        were just completed.
        """
        assert (
            key in self.state
        ), f"error: used key {key}, which isn't in ({list(self.state.keys())})"
        self.state[key].update_completed(delta_num_items_completed)
        self.event.set()

    def update_total(self, key: str, delta_total: int) -> None:
        """
        Updates the ProgressBar with the incremental number of items that
        represent new work, still to be done.
        """
        assert (
            key in self.state
        ), f"error: used key {key}, which isn't in ({list(self.state.keys())})"
        self.state[key].update_total(delta_total)
        self.event.set()

    async def wait_for_update(self) -> Dict[str, ProgressBarState]:
        """
        Blocking call: waits until somebody calls `update_completed` or `update_total`,
        then returns the progress bar state.
        """
        await self.event.wait()
        self.event.clear()
        return self.state

    def get_state(self) -> Dict[str, ProgressBarState]:
        """
        Non-blocking call: fetches the state immediately.
        """
        return self.state


class ProgressBar:
    """
    This is where the progress bar starts. You create one of these on the head node,
    passing in the expected total number of items and a description key for each one.
    This will then manage one `tqdm` counter for each, simultaneously. For example,
    if you have 100 "Ballots" and 10 "Tallies", you might make
    `ProgressBar({"Ballots": 100, "Tallies": 10})`.

    Pass along the `actor` reference to any remote task. If, for example, the task just
    completed three "Ballots", it would then call: `actor.update_total.remote("Ballots", 3)`.

    Back on the head node, once you launch your remote Ray tasks, call `print_until_done()`,
    which will then print all the `tqdm` counters as they evolve, and will return when
    every counter has reached its specified total.

    If your program is the sort that discovers more work to do as it goes along, you can
    use the actor's `update_total` method.
    """

    progress_actor: ActorHandle
    totals: Dict[str, int]
    progress_bars: Dict[str, tqdm]

    def __init__(self, totals: Dict[str, int]):
        # Ray actors don't seem to play nice with mypy, generating a spurious warning
        # for the following line, which we need to suppress. The code is fine.
        self.progress_actor = ProgressBarActor.remote(totals)  # type: ignore
        self.totals = totals
        self.progress_bars = {
            key: tqdm(desc=key, total=self.totals[key]) for key in self.totals.keys()
        }

    @property
    def actor(self) -> ActorHandle:
        """
        Returns a reference to the remote `ProgressBarActor`. When you complete tasks,
        call `update` on the actor.
        """
        return self.progress_actor

    def print_update(self, wait_for_update: bool = False) -> bool:
        """
        If requested via the `wait_for_update` flag, this will wait until there's any update in the
        state of the job. Then, either way, it updates the progress bars and return. If the job is done, this
        will return True and close the progress bars. If not, it returns False.
        """

        if not self.progress_bars:
            # somebody already called close(), so we're done
            return True
        else:
            if wait_for_update:
                state = ray.get(self.actor.wait_for_update.remote())
            else:
                state = ray.get(self.actor.get_state.remote())

            complete = True
            for k in state.keys():
                s: ProgressBarState = state[k]
                p: tqdm = self.progress_bars[k]
                p.n = s.counter
                p.total = s.total
                p.refresh()
                complete = complete and s.counter >= s.total
            if complete:
                self.close()
            return complete

    def print_until_done(self) -> None:
        """
        Blocking call, runs for a while. Do this after starting a series of remote Ray tasks,
        to which you've passed the actor handle. Your remote workers might then call the `update` methods
        on the actor. When the progress meter reaches 100%, this method returns.
        """
        while not self.print_update(wait_for_update=True):
            pass

    def close(self) -> None:
        """
        If you know the work is done, this calls `close` on the progressbars within.
        """
        for pb in self.progress_bars.values():
            # sometimes, the computation is done, but the updates haven't arrived at
            # the actor in time to get here. We're just going to set the counters to
            # match the totals, so everything ends up looking good.
            if pb.total > 0:
                pb.n = pb.total
            else:
                pb.total = pb.n
            pb.refresh()

        for pb in self.progress_bars.values():
            pb.close()

        self.progress_bars = {}
