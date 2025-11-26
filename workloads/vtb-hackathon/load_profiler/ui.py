from datetime import datetime
from colorama import Fore, Style
from tabulate import tabulate
from config import RECOMMENDATIONS

def print_dashboard(profile, confidence, metrics):
    print("\033c", end="") # Очистка консоли
    print(f"{Style.BRIGHT}{Fore.CYAN}=== VTB HACKATHON: Load Profile Detector (Modular) ==={Style.RESET_ALL}")
    print(f"Статус на: {datetime.now().strftime('%H:%M:%S')}")
    print("-" * 50)
    
    # Цвет профиля
    p_color = Fore.GREEN if "OLTP" in profile else Fore.YELLOW if "Mixed" in profile else Fore.MAGENTA
    print(f"DETECTED PROFILE: {Style.BRIGHT}{p_color}{profile}{Style.RESET_ALL} (Confidence: {confidence})")
    
    # Таблица метрик
    table_data = [[k, v] for k, v in metrics.items()]
    print(tabulate(table_data, headers=["Metric", "Value"], tablefmt="fancy_grid"))
    
    # Рекомендации
    base_profile = profile.split(" (")[0]
    if base_profile in RECOMMENDATIONS:
        print(f"\n{Fore.WHITE}{Style.BRIGHT}Рекомендации (postgresql.conf):{Style.RESET_ALL}")
        recs = RECOMMENDATIONS[base_profile]
        for param, val in recs.items():
            print(f"  {Fore.YELLOW}• {param} = {val}{Style.RESET_ALL}")
    else:
        print(f"\n{Fore.RED}Нет специфичных рекомендаций.{Style.RESET_ALL}")