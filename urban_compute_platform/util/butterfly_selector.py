import random
import time
from typing import List, Optional, Dict
from uuid import UUID

from urban_compute_platform.model import Capabilities
from urban_compute_platform.nodetypes.executor import Executor
from urban_compute_platform.util.log import LOG


class ButterflyExecutorSelector:
    """Butterfly Optimization Algorithm (BOA) zur Auswahl des optimalen Executors"""
    
    # Konfigurationsparameter
    POPULATION_SIZE = 30
    MAX_ITERATIONS = 10
    SENSORY_MODALITY = 0.01  # Bestimmt die Qualität des Dufts (c)
    POWER_EXPONENT = 0.1     # Steuert die Absorption des Dufts (a)
    MIN_SENSORY = 0.001      # Minimaler Wert für Sensory Modality
    
    # Cache für Executor-Performance
    performance_history: Dict[UUID, float] = {}
    last_job_times: Dict[UUID, float] = {}
    
    # Fitness-History über Iterationen
    best_fitness_history: List[float] = []
    avg_fitness_history: List[float] = []
    
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
        if executor.id in ButterflyExecutorSelector.performance_history:
            history_factor = ButterflyExecutorSelector.performance_history[executor.id]
        
        # Zeit seit letztem Job
        recency_factor = 1.0
        current_time = time.time()
        if executor.id in ButterflyExecutorSelector.last_job_times:
            time_since_last_job = current_time - ButterflyExecutorSelector.last_job_times[executor.id]
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
    def _calculate_fragrance(stimulus_intensity: float, sensory_modality: float) -> float:
        """
        Berechnet den Duft-Wert nach Gleichung: f = c * I^a
        """
        return sensory_modality * (stimulus_intensity ** ButterflyExecutorSelector.POWER_EXPONENT)
    
    @staticmethod
    def _global_search(current_idx: int, best_idx: int, fragrance: float, population_size: int) -> int:
        """
        Globale Suche nach Gleichung: x(t+1) = x(t) + (r^2 * g* - x(t)) * f
        """
        r = random.random()
        move = int(fragrance * (r**2 * best_idx - current_idx))
        new_idx = current_idx + move
        return max(0, min(population_size - 1, new_idx))
    
    @staticmethod
    def _local_search(current_idx: int, j_idx: int, k_idx: int, fragrance: float, population_size: int) -> int:
        """
        Lokale Suche nach Gleichung: x(t+1) = x(t) + (r^2 * x_j - x_k) * f
        """
        r = random.random()
        move = int(fragrance * (r**2 * j_idx - k_idx))
        new_idx = current_idx + move
        return max(0, min(population_size - 1, new_idx))
    
    @staticmethod
    def select_executor(executors: List[Executor], capabilities: Capabilities) -> Optional[Executor]:
        """Wählt einen Executor mit Butterfly-Optimierungsalgorithmus aus"""
        if not executors:
            return None
        
        if len(executors) == 1:
            return executors[0]
        
        # Initialisiere Population mit zufälligen Indizes
        population = [random.randint(0, len(executors) - 1) for _ in range(ButterflyExecutorSelector.POPULATION_SIZE)]
        
        # Berechne Fitness für jedes Individuum
        fitness_scores = [ButterflyExecutorSelector.calculate_fitness(executors[idx], capabilities) 
                         for idx in population]
        
        # Finde bestes Individuum
        best_idx_in_pop = fitness_scores.index(max(fitness_scores))
        best_idx = population[best_idx_in_pop]
        
        sensory_modality = ButterflyExecutorSelector.SENSORY_MODALITY
        
        # Hauptschleife des Butterfly-Algorithmus
        for iteration in range(ButterflyExecutorSelector.MAX_ITERATIONS):
            best_fitness_in_iter = max(fitness_scores)
            avg_fitness_in_iter = sum(fitness_scores) / len(fitness_scores)
            
            # Für jeden Schmetterling in der Population
            for i in range(ButterflyExecutorSelector.POPULATION_SIZE):
                # Berechne Duft
                fragrance = ButterflyExecutorSelector._calculate_fragrance(
                    fitness_scores[i], 
                    sensory_modality
                )
                
                # Entscheide zwischen globaler und lokaler Suche
                if random.random() < sensory_modality:
                    # Globale Suche
                    new_idx = ButterflyExecutorSelector._global_search(
                        population[i], 
                        best_idx, 
                        fragrance,
                        len(executors)
                    )
                else:
                    # Lokale Suche
                    j, k = random.sample(range(ButterflyExecutorSelector.POPULATION_SIZE), 2)
                    new_idx = ButterflyExecutorSelector._local_search(
                        population[i],
                        population[j],
                        population[k],
                        fragrance,
                        len(executors)
                    )
                
                # Berechne Fitness für neue Position
                new_fitness = ButterflyExecutorSelector.calculate_fitness(executors[new_idx], capabilities)
                
                # Update falls besser
                if new_fitness > fitness_scores[i]:
                    population[i] = new_idx
                    fitness_scores[i] = new_fitness
                    
                    # Update bestes Individuum falls nötig
                    if new_fitness > fitness_scores[best_idx_in_pop]:
                        best_idx_in_pop = i
                        best_idx = population[i]
            
            # Reduziere sensory_modality (simuliert Duft-Abnahme)
            sensory_modality = max(
                sensory_modality * 0.95,
                ButterflyExecutorSelector.MIN_SENSORY
            )
            
            # Speichere Fitness-Historie
            ButterflyExecutorSelector.best_fitness_history.append(best_fitness_in_iter)
            ButterflyExecutorSelector.avg_fitness_history.append(avg_fitness_in_iter)
        
        # Besten Executor auswählen
        selected_executor = executors[best_idx]
        ButterflyExecutorSelector.last_job_times[selected_executor.id] = time.time()
        
        LOG.debug(f"Butterfly algorithm selected executor {selected_executor.id} with fitness {max(fitness_scores)}")
        
        return selected_executor
    
    @staticmethod
    def update_performance(executor_id: UUID, success_score: float) -> None:
        """Aktualisiert die Performance-Historie nach Abschluss eines Jobs"""
        if executor_id in ButterflyExecutorSelector.performance_history:
            ButterflyExecutorSelector.performance_history[executor_id] = (
                0.75 * ButterflyExecutorSelector.performance_history[executor_id] + 
                0.25 * success_score
            )
        else:
            ButterflyExecutorSelector.performance_history[executor_id] = success_score
    
    @staticmethod
    def get_fitness_history() -> Dict[str, List[float]]:
        """Gibt die Fitness-Historie zurück"""
        return {
            "best_fitness": ButterflyExecutorSelector.best_fitness_history,
            "avg_fitness": ButterflyExecutorSelector.avg_fitness_history
        }