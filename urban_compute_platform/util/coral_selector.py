import random
import time
from typing import List, Optional, Dict, Tuple
from uuid import UUID

from urban_compute_platform.model import Capabilities
from urban_compute_platform.nodetypes.executor import Executor
from urban_compute_platform.util.log import LOG


class CoralExecutorSelector:
    """Coral Reefs Optimization (CRO) zur Auswahl des optimalen Executors"""
    
    # Konfigurationsparameter
    REEF_SIZE = (5, 5)      # Größe des Riffs (N, M)
    RHO = 0.6               # Verhältnis von besetzten/leeren Positionen zu Beginn
    FB = 0.8                # Broadcast-Spawning-Rate
    FA = 0.1                # Prozentsatz der besten Korallen für Knospung
    FD = 0.1                # Anteil der schlechtesten Korallen für Eliminierung
    ATTEMPTS = 3            # Maximale Versuche, sich im Riff anzusiedeln
    MAX_ITERATIONS = 5      # Maximale Anzahl von Iterationen
    CROSSOVER_RATE = 0.7    # Wahrscheinlichkeit für Crossover
    MUTATION_RATE = 0.1     # Wahrscheinlichkeit für Mutation
    
    # Cache für Executor-Performance
    performance_history: Dict[UUID, float] = {}
    last_job_times: Dict[UUID, float] = {}
    
    @staticmethod
    def calculate_fitness(executor: Executor, capabilities: Capabilities) -> float:
        """Berechnet die Fitness eines Executors"""
        caps = executor.cur_caps
        
        # Ressourcen-basierte Fitness
        memory_ratio = caps.memory / max(1, capabilities.memory)
        disk_ratio = caps.disk / max(1, capabilities.disk)
        cpu_ratio = (100 - caps.cpu_load) / 100
        
        # Performance-Historie
        history_factor = 1.0
        if executor.id in CoralExecutorSelector.performance_history:
            history_factor = CoralExecutorSelector.performance_history[executor.id]
        
        # Zeit seit letztem Job
        recency_factor = 1.0
        current_time = time.time()
        if executor.id in CoralExecutorSelector.last_job_times:
            time_since_last_job = current_time - CoralExecutorSelector.last_job_times[executor.id]
            recency_factor = min(2.0, 1.0 + (time_since_last_job / 3600))
        
        # Batterie-Faktor
        battery_factor = 1.0
        if caps.has_battery:
            battery_factor = caps.power / 100
        
        # Kombinierte Fitness
        fitness = (
            0.3 * memory_ratio + 
            0.2 * disk_ratio + 
            0.3 * cpu_ratio + 
            0.1 * history_factor + 
            0.1 * recency_factor
        ) * battery_factor
        
        return fitness
    
    @staticmethod
    def _create_coral(idx: int, fitness: float) -> Dict:
        """Erstellt eine neue Koralle"""
        return {
            "idx": idx,
            "fitness": fitness,
            "age": 0
        }
    
    @staticmethod
    def _broadcast_spawning(reef: Dict, executors: List[Executor], 
                         capabilities: Capabilities) -> List[Dict]:
        """Externe sexuelle Reproduktion über Broadcast-Spawning"""
        larvae = []
        try:
            # Korallen für Reproduktion auswählen
            corals = list(reef.values())
            num_broadcast = int(len(corals) * CoralExecutorSelector.FB)
            if num_broadcast < 2:
                num_broadcast = min(2, len(corals))
            broadcast_corals = random.sample(corals, num_broadcast)
            
            # Larven durch Crossover erstellen
            for i in range(0, len(broadcast_corals)-1, 2):
                if random.random() < CoralExecutorSelector.CROSSOVER_RATE:
                    p1 = broadcast_corals[i]
                    p2 = broadcast_corals[i+1]
                    
                    # Neuer Index zwischen den Eltern-Indizes
                    child_idx = (p1["idx"] + p2["idx"]) // 2
                    # Sicherstellen, dass Index im gültigen Bereich liegt
                    child_idx = max(0, min(len(executors) - 1, child_idx))
                    
                    # Fitness berechnen
                    fitness = CoralExecutorSelector.calculate_fitness(executors[child_idx], capabilities)
                    larvae.append(CoralExecutorSelector._create_coral(child_idx, fitness))
        except Exception as e:
            LOG.error(f"Error in broadcast spawning: {str(e)}")
        
        return larvae
    
    @staticmethod
    def _brooding(reef: Dict, executors: List[Executor], capabilities: Capabilities) -> List[Dict]:
        """Interne sexuelle Reproduktion durch Brooding (Mutation)"""
        larvae = []
        try:
            # Korallen für Brooding auswählen
            corals = list(reef.values())
            num_brooders = int(len(corals) * (1 - CoralExecutorSelector.FB))
            if num_brooders < 1:
                num_brooders = min(1, len(corals))
            brood_corals = random.sample(corals, num_brooders)
            
            for coral in brood_corals:
                if random.random() < CoralExecutorSelector.MUTATION_RATE:
                    # Mutierte Position erstellen (leicht verschobener Index)
                    delta = random.randint(-2, 2)
                    child_idx = coral["idx"] + delta
                    # Sicherstellen, dass Index im gültigen Bereich liegt
                    child_idx = max(0, min(len(executors) - 1, child_idx))
                    
                    # Fitness berechnen
                    fitness = CoralExecutorSelector.calculate_fitness(executors[child_idx], capabilities)
                    larvae.append(CoralExecutorSelector._create_coral(child_idx, fitness))
        except Exception as e:
            LOG.error(f"Error in brooding: {str(e)}")
        
        return larvae
    
    @staticmethod
    def _larvae_setting(reef: Dict, larvae: List[Dict]) -> Dict:
        """Larven versuchen, sich im Riff anzusiedeln"""
        N, M = CoralExecutorSelector.REEF_SIZE
        reef_size = N * M
        
        for larva in larvae:
            settled = False
            attempts = 0
            
            # Versuchen anzusiedeln, bis erfolgreich oder max. Versuche erreicht
            while not settled and attempts < CoralExecutorSelector.ATTEMPTS:
                pos = (random.randrange(N), random.randrange(M))
                pos_str = f"{pos[0]},{pos[1]}"
                
                # Prüfen, ob Position leer ist
                if pos_str not in reef:
                    reef[pos_str] = larva
                    settled = True
                # Wenn besetzt, um Platz konkurrieren
                elif larva["fitness"] > reef[pos_str]["fitness"]:
                    reef[pos_str] = larva
                    settled = True
                
                attempts += 1
                
                # Wenn Riff voll ist, einen Platz freimachen
                if len(reef) >= reef_size and not settled:
                    worst_pos = min(reef.items(), key=lambda x: x[1]["fitness"])[0]
                    if reef[worst_pos]["fitness"] < larva["fitness"]:
                        reef.pop(worst_pos)
        
        return reef
    
    @staticmethod
    def _budding(reef: Dict, executors: List[Executor], capabilities: Capabilities) -> Dict:
        """Asexuelle Reproduktion der besten Korallen"""
        N, M = CoralExecutorSelector.REEF_SIZE
        
        try:
            # Korallen nach Fitness sortieren
            sorted_corals = sorted(reef.items(), key=lambda x: x[1]["fitness"], reverse=True)
            
            # Beste Korallen für Knospung auswählen
            num_duplicate = int(len(sorted_corals) * CoralExecutorSelector.FA)
            if num_duplicate < 1 and sorted_corals:
                num_duplicate = 1
            
            best_corals = sorted_corals[:num_duplicate]
            
            # Versuchen, jede Koralle zu duplizieren
            for _, coral in best_corals:
                settled = False
                attempts = 0
                
                while not settled and attempts < CoralExecutorSelector.ATTEMPTS:
                    pos = (random.randrange(N), random.randrange(M))
                    pos_str = f"{pos[0]},{pos[1]}"
                    
                    if pos_str not in reef:
                        # Leichte Mutation beim Klonen
                        delta = random.randint(-1, 1)
                        idx = max(0, min(len(executors) - 1, coral["idx"] + delta))
                        
                        fitness = CoralExecutorSelector.calculate_fitness(executors[idx], capabilities)
                        reef[pos_str] = CoralExecutorSelector._create_coral(idx, fitness)
                        settled = True
                    
                    attempts += 1
        except Exception as e:
            LOG.error(f"Error in budding: {str(e)}")
            
        return reef
    
    @staticmethod
    def _depredation(reef: Dict) -> Dict:
        """Entfernen der schlechtesten Korallen aus dem Riff"""
        try:
            # Korallen nach Fitness sortieren (schlechteste zuerst)
            sorted_corals = sorted(reef.items(), key=lambda x: x[1]["fitness"])
            
            # Schlechteste Korallen entfernen
            num_remove = int(len(sorted_corals) * CoralExecutorSelector.FD)
            for pos_str, _ in sorted_corals[:num_remove]:
                reef.pop(pos_str)
        except Exception as e:
            LOG.error(f"Error in depredation: {str(e)}")
        
        return reef
    
    @staticmethod
    def select_executor(executors: List[Executor], capabilities: Capabilities) -> Optional[Executor]:
        """Wählt einen Executor mit Coral-Reefs-Optimierungsalgorithmus aus"""
        if not executors:
            return None
        
        if len(executors) == 1:
            return executors[0]
        
        N, M = CoralExecutorSelector.REEF_SIZE
        reef = {}
        
        # Initialisiere Riff mit zufälligen Korallen
        num_corals = int(N * M * CoralExecutorSelector.RHO)
        positions = set()
        
        for _ in range(min(num_corals, N * M)):
            pos = (random.randrange(N), random.randrange(M))
            pos_str = f"{pos[0]},{pos[1]}"
            
            if pos_str not in positions:
                positions.add(pos_str)
                
                # Zufälligen Executor auswählen
                idx = random.randint(0, len(executors) - 1)
                fitness = CoralExecutorSelector.calculate_fitness(executors[idx], capabilities)
                
                reef[pos_str] = CoralExecutorSelector._create_coral(idx, fitness)
        
        # Beste Lösung initialisieren
        best_solution = None
        best_fitness = float('-inf')
        
        # Hauptoptimierungsschleife
        for iteration in range(CoralExecutorSelector.MAX_ITERATIONS):
            # Externe Reproduktion (Broadcast-Spawning)
            larvae = CoralExecutorSelector._broadcast_spawning(reef, executors, capabilities)
            
            # Interne Reproduktion (Brooding)
            larvae.extend(CoralExecutorSelector._brooding(reef, executors, capabilities))
            
            # Larven versuchen, sich im Riff anzusiedeln
            reef = CoralExecutorSelector._larvae_setting(reef, larvae)
            
            # Asexuelle Reproduktion
            reef = CoralExecutorSelector._budding(reef, executors, capabilities)
            
            # Schlechteste Lösungen entfernen
            reef = CoralExecutorSelector._depredation(reef)
            
            # Beste Lösung aktualisieren
            for coral in reef.values():
                if coral["fitness"] > best_fitness:
                    best_fitness = coral["fitness"]
                    best_solution = coral
            
            LOG.debug(f"CRO Iteration {iteration+1}/{CoralExecutorSelector.MAX_ITERATIONS}, "
                     f"Best fitness: {best_fitness:.4f}, Reef size: {len(reef)}")
        
        if best_solution is None:
            # Fallback: Zufällige Auswahl
            return random.choice(executors)
        
        # Ausgewählten Executor zurückgeben
        selected_executor = executors[best_solution["idx"]]
        CoralExecutorSelector.last_job_times[selected_executor.id] = time.time()
        
        LOG.debug(f"Coral Reefs algorithm selected executor {selected_executor.id} with fitness {best_fitness}")
        
        return selected_executor
    
    @staticmethod
    def update_performance(executor_id: UUID, success_score: float) -> None:
        """Aktualisiert die Performance-Historie nach Abschluss eines Jobs"""
        if executor_id in CoralExecutorSelector.performance_history:
            CoralExecutorSelector.performance_history[executor_id] = (
                0.75 * CoralExecutorSelector.performance_history[executor_id] + 
                0.25 * success_score
            )
        else:
            CoralExecutorSelector.performance_history[executor_id] = success_score