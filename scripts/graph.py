from collections import defaultdict, deque


class DependencyGraph:
    def __init__(self, config: dict):
        self.files = config["files"]
        self.graph = defaultdict(list)
        self.in_degree = defaultdict(int)

        self._validate()
        self._build()

    def _validate(self):
        for name, cfg in self.files.items():
            for dep in cfg.get("depends_on", []):
                if dep not in self.files:
                    raise ValueError(f"Unknown dependency: {dep}")

                if dep == name:
                    raise ValueError(f"Self dependency: {name}")

    def _build(self):
        for name in self.files:
            self.in_degree[name] = 0

        for name, cfg in self.files.items():
            for dep in cfg.get("depends_on", []):
                self.graph[dep].append(name)
                self.in_degree[name] += 1

    def get_levels(self):
        in_degree = dict(self.in_degree)
        levels = []

        queue = [n for n in in_degree if in_degree[n] == 0]

        while queue:
            current_level = queue
            levels.append(current_level)

            next_queue = []

            for node in current_level:
                for neighbor in self.graph[node]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        next_queue.append(neighbor)

            queue = next_queue

        if sum(len(l) for l in levels) != len(self.files):
            raise ValueError("Cycle detected in dependencies")

        return levels
