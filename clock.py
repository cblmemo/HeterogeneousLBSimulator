TICK_PERIOD_S = 0.1
# TICK_PERIOD_S = 10.0


class Clock:
    def __init__(self) -> None:
        # Start from -1, since we will increment it in the first step.
        self.tick = -1

    def step(self) -> int:
        self.tick += 1
        return self.tick
