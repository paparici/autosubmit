# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit. If not, see <http://www.gnu.org/licenses/>.

class CPMIPMetrics:
    """Evaluates CPMIP performance metrics against configured thresholds."""

    @staticmethod
    def SY(chunk_size, chunk_size_unit) -> float:
        """Convert chunk size and unit into simulated years.

        :param chunk_size: Size of the simulation chunk.
        :param chunk_size_unit: Unit of chunk_size. Supported values are year, month, day, hour.
        :return: Simulated years represented by the chunk.
        :raises TypeError: If chunk_size_unit is not a string.
        :raises ValueError: If chunk_size_unit is unsupported or chunk_size is not positive.
        """
        if not isinstance(chunk_size_unit, str):
            raise TypeError("chunk_size_unit must be a string")

        unit_to_years = {
            "year": 1.0,
            "month": 1.0 / 12.0,
            "day": 1.0 / 365.0,
            "hour": 1.0 / 8760.0,
        }

        unit = chunk_size_unit.strip().lower()
        if unit not in unit_to_years:
            raise ValueError(f"Unsupported chunk_size_unit: {chunk_size_unit}")

        size = float(chunk_size)
        if size <= 0:
            raise ValueError("chunk_size must be > 0")

        return size * unit_to_years[unit]

    @staticmethod
    def SYPD(runtime, simulated_years) -> float:
        """Compute Simulated Years Per Day from runtime and simulated years.

        :param runtime: Runtime in hours.
        :param simulated_years: Simulated years.
        :return: Simulated years per day.
        :raises ValueError: If runtime is not positive or simulated_years is not positive.
        """
        runtime_hours = float(runtime)
        if runtime_hours <= 0:
            raise ValueError("runtime must be > 0 hours")

        sy = float(simulated_years)
        if sy <= 0:
            raise ValueError("simulated_years must be > 0")

        return sy * 24.0 / runtime_hours

    @staticmethod
    def CHSY(runtime, simulated_years, total_processors) -> float:
        """Compute Core-Hours per Simulated Year.

        :param runtime: Runtime in hours.
        :param simulated_years: Simulated years.
        :param total_processors: Number of processors used by the job.
        :return: Core-hours per simulated year.
        :raises ValueError: If any input is not positive.
        """
        runtime_hours = float(runtime)
        if runtime_hours <= 0:
            raise ValueError("runtime must be > 0 hours")

        sy = float(simulated_years)
        if sy <= 0:
            raise ValueError("simulated_years must be > 0")

        processors = float(total_processors)
        if processors <= 0:
            raise ValueError("total_processors must be > 0")

        return processors * runtime_hours / sy

    @staticmethod
    def _get_runtime_hours(job):
        """Extract runtime in hours from job timestamps.

        :param job: Job-like object with start/finish timestamps.
        :return: Runtime in hours or ``None`` when unavailable/invalid.
        """
        start_time = getattr(job, "start_time_timestamp", None)
        finish_time = getattr(job, "finish_time_timestamp", None)
        if start_time is None or finish_time is None:
            return None

        try:
            runtime_seconds = float(finish_time) - float(start_time)
        except (ValueError, TypeError):
            return None

        if runtime_seconds <= 0:
            return None

        return runtime_seconds / 3600.0

    @staticmethod
    def _get_simulated_years(job):
        """Extract simulated years from job chunk metadata.

        :param job: Job-like object with ``chunk_size`` and ``chunk_size_unit``.
        :return: Simulated years or ``None`` when metadata is unavailable/invalid.
        """
        chunk_size = getattr(job, "chunk_size", None)
        chunk_size_unit = getattr(job, "chunk_size_unit", None)
        if chunk_size is None or chunk_size_unit is None:
            return None

        try:
            return CPMIPMetrics.SY(chunk_size, chunk_size_unit)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _get_processors_for_chsy(job):
        """Resolve processor count for CHSY computation.

        Uses the job ``total_processors`` property as the source of truth.

        :param job: Job-like object.
        :return: Processor count or ``None`` when unavailable/invalid.
        """
        try:
            return getattr(job, "total_processors", None)
        except (ValueError, TypeError, AttributeError):
            return None

    @staticmethod
    def _fetch_metrics(job) -> dict:
        """Build CPMIP metrics from job runtime/chunk metadata.

        :param job: Job object.
        :return: Dictionary with computed metrics. Empty dict when required metadata is missing or invalid.
        """
        runtime_hours = CPMIPMetrics._get_runtime_hours(job)
        simulated_years = CPMIPMetrics._get_simulated_years(job)
        if runtime_hours is None or simulated_years is None:
            return {}

        metrics = {}

        try:
            metrics["SYPD"] = CPMIPMetrics.SYPD(runtime_hours, simulated_years)
        except (ValueError, TypeError):
            pass

        total_processors = CPMIPMetrics._get_processors_for_chsy(job)

        if total_processors is not None:
            try:
                metrics["CHSY"] = CPMIPMetrics.CHSY(runtime_hours, simulated_years, total_processors)
            except (ValueError, TypeError):
                pass

        return metrics

    @staticmethod
    def evaluate(job, thresholds) -> dict:
        """Evaluate job metrics against threshold definitions.

        :param job: Job-like object used to fetch computed metrics.
        :param thresholds: Threshold configuration by metric name.
        :return: Violations by metric name.
        """
        if not thresholds:
            return {}

        metrics = CPMIPMetrics._fetch_metrics(job)
        if not metrics:
            return {}

        violations = {}

        for metric_name, threshold_config in thresholds.items():
            if metric_name not in metrics:
                continue
            
            try:
                threshold = float(threshold_config["THRESHOLD"])
                comparison = str(threshold_config["COMPARISON"]).strip().lower()
                accepted_error = float(threshold_config.get("%_ACCEPTED_ERROR", 0))
            except (ValueError, TypeError, KeyError):
                continue

            real_value = metrics[metric_name]
            tolerance_factor = accepted_error / 100.0
            is_violation = False

            if comparison == "greater_than":
                lower_bound = threshold * (1 - tolerance_factor)
                bound = lower_bound
                if real_value < lower_bound:
                    is_violation = True
            elif comparison == "less_than":
                upper_bound = threshold * (1 + tolerance_factor)
                bound = upper_bound
                if real_value > upper_bound:
                    is_violation = True
            else:
                continue

            if is_violation:
                violations[metric_name] = {
                    "threshold": threshold,
                    "accepted_error": accepted_error,
                    "comparison": comparison,
                    "bound": bound,
                    "real_value": real_value,
                }

        return violations