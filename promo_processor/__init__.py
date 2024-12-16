import importlib
import pkgutil
from pathlib import Path
from promo_processor.processor import PromoProcessor


__all__ = []

package_dir = Path(__file__).parent / "processors"

def load_processors():
    for (_, module_name, _) in pkgutil.iter_modules([package_dir]):
        module = importlib.import_module(f"{__package__}.processors.{module_name}")
        if hasattr(module, '__all__'):
            __all__.extend(module.__all__)
        else:
            __all__.extend([attr for attr in dir(module) 
                          if not attr.startswith('_') 
                          and isinstance(getattr(module, attr), type)
                          and issubclass(getattr(module, attr), PromoProcessor)])

load_processors()