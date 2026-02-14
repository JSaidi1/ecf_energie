import psutil
from pathlib import Path

def get_machine_available_resources(interval=2):
    """
    Measure current machine resource availability.

    This function samples system metrics over a short time window and returns
    an estimate of the machine's currently available resources. It is designed
    for capacity checks before launching heavy workloads.

    Metrics returned:
        - ram_available_gb: Available system RAM in gigabytes.
        - cpu_available_pct: Estimated available CPU percentage
          (100 - average CPU usage during the sampling interval).
        - available_logical_cores: Approximate number of free logical cores.
        - available_physical_cores: Approximate number of free physical cores.

    Notes:
        - RAM availability is an exact system value.
        - CPU availability and core counts are estimates based on recent usage.
        - CPU is sampled over `interval` seconds (this call blocks for that duration).

    Args:
        interval (float): Duration in seconds used to sample CPU usage.
                          Higher values give more stable metrics but block longer.
                          Typical values: 1-2 seconds.

    Example:
        metrics = get_machine_available_resources()

        print(metrics["ram_available_gb"])
        
        print(metrics["available_logical_cores"])

    Returns:
        dict: Dictionary containing the resource metrics.
	"""
    
    # RAM disponible (GB)
    ram_available = psutil.virtual_memory().available / (1024**3)

    # Cores
    logical = psutil.cpu_count()
    physical = psutil.cpu_count(logical=False) or logical

    # CPU disponible (%)
    cpu_used = psutil.cpu_percent(interval=interval)
    cpu_available_pct = 100 - cpu_used

    # Cores disponibles (approx)
    available_logical = logical * cpu_available_pct / 100
    available_physical = physical * cpu_available_pct / 100

    return {
        "ram_available_gb": round(ram_available, 2),
        "cpu_available_pct": round(cpu_available_pct, 1),
        "available_logical_cores": round(available_logical, 2),
        "available_physical_cores": round(available_physical, 2),
    }


