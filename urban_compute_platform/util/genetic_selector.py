import random
import time
from typing import List, Optional, Tuple, Dict
from uuid import UUID

from urban_compute_platform.model import Capabilities
from urban_compute_platform.nodetypes.executor import Executor
from urban_compute_platform.util.log import LOG


class GeneticExecutorSelector:
    """Verwendet genetischen Algorithmus zur Auswahl des optimalen Executors"""
    
    # Konfigurationsparameter
    POPULATION_SIZE = 20
    GENERATIONS = 5
    MUTATION_RATE = 0.1
    CROSSOVER_RATE = 0.7
    
    # Cache für Executor-Performance
    performance_history: Dict[UUID, float] = {}
    last_job_times: Dict[UUID, float] = {}
    
    # Fitness-History über Generationen
    best_fitness_history: List[float] = []
    avg_fitness_history: List[float] = []
    
    @staticmethod
    def calculate_fitness(executor: Executor, capabilities: Capabilities) -> float:
        """Berechnet die Fitness eines Executors für den gegebenen Job"""
        caps = executor.cur_caps
        
        # Grundlegende Fitness basierend auf verfügbaren Ressourcen
        memory_ratio = caps.memory / max(1, capabilities.memory)  # Höher ist besser
        disk_ratio = caps.disk / max(1, capabilities.disk)  # Höher ist besser
        cpu_ratio = (100 - caps.cpu_load) / 100  # Niedriger Load ist besser
        
        # Berücksichtige frühere Performance
        history_factor = 1.0
        if executor.id in GeneticExecutorSelector.performance_history:
            history_factor = GeneticExecutorSelector.performance_history[executor.id]
        
        # Berücksichtige Alter des letzten Jobs (um Load-Balancing zu fördern)
        recency_factor = 1.0
        current_time = time.time()
        if executor.id in GeneticExecutorSelector.last_job_times:
            time_since_last_job = current_time - GeneticExecutorSelector.last_job_times[executor.id]
            # Höherer Faktor für Executors, die länger keinen Job hatten
            recency_factor = min(2.0, 1.0 + (time_since_last_job / 3600))
        
        # Batteriefaktor (bevorzuge Geräte mit Netzstrom oder hoher Batterie)
        battery_factor = 1.0
        if caps.has_battery:
            battery_factor = caps.power / 100
        
        # Kombinierte Fitness (gewichtete Summe)
        fitness = (
            0.3 * memory_ratio + 
            0.2 * disk_ratio + 
            0.3 * cpu_ratio + 
            0.1 * history_factor + 
            0.1 * recency_factor
        ) * battery_factor
        
        return fitness
    
    @staticmethod
    def crossover(parent1: List[int], parent2: List[int]) -> Tuple[List[int], List[int]]:
        """Führt Crossover zwischen zwei Eltern durch"""
        if random.random() < GeneticExecutorSelector.CROSSOVER_RATE and len(parent1) > 1:
            # Single-point crossover
            crossover_point = random.randint(1, len(parent1) - 1)
            child1 = parent1[:crossover_point] + parent2[crossover_point:]
            child2 = parent2[:crossover_point] + parent1[crossover_point:]
            return child1, child2
        return parent1.copy(), parent2.copy()
    
    @staticmethod
    def select_executor(executors: List[Executor], capabilities: Capabilities) -> Optional[Executor]:
        """Wählt einen Executor mit genetischem Algorithmus aus"""
        if not executors:
            return None
        
        if len(executors) == 1:
            return executors[0]
        
        # Initialisiere Population (zufällige Auswahl von Executors)
        population = []
        for _ in range(GeneticExecutorSelector.POPULATION_SIZE):
            # Ein Individuum ist ein Index in die executor-Liste
            population.append(random.randint(0, len(executors) - 1))
        
        # Fitness-History für diese Ausführung zurücksetzen
        best_fitness_per_gen = []
        avg_fitness_per_gen = []
        
        # Evolution über mehrere Generationen
        for generation in range(GeneticExecutorSelector.GENERATIONS):
            # Berechne Fitness für jedes Individuum
            fitness_scores = [
                GeneticExecutorSelector.calculate_fitness(executors[idx], capabilities) 
                for idx in population
            ]
            
            # Fitness-Statistiken für diese Generation speichern
            best_fitness_per_gen.append(max(fitness_scores))
            avg_fitness_per_gen.append(sum(fitness_scores) / len(fitness_scores))
            
            # Selektion (wähle die besten Individuen als Eltern aus)
            selection_probs = [f/max(sum(fitness_scores), 0.001) for f in fitness_scores]
            
            # Erstelle Elternpaare
            parents = []
            for _ in range(GeneticExecutorSelector.POPULATION_SIZE):
                selected_idx = random.choices(range(len(population)), weights=selection_probs)[0]
                parents.append(population[selected_idx])
            
            # Erzeuge neue Population durch Crossover und Mutation
            new_population = []
            for i in range(0, len(parents), 2):
                if i + 1 < len(parents):
                    # Crossover anwenden
                    child1, child2 = GeneticExecutorSelector.crossover([parents[i]], [parents[i+1]])
                    new_population.extend(child1)
                    if len(new_population) < GeneticExecutorSelector.POPULATION_SIZE:
                        new_population.extend(child2)
            
            # Fülle die Population auf, falls nötig
            while len(new_population) < GeneticExecutorSelector.POPULATION_SIZE:
                new_population.append(random.randint(0, len(executors) - 1))
            
            # Mutation (kleine zufällige Änderungen)
            for i in range(len(new_population)):
                if random.random() < GeneticExecutorSelector.MUTATION_RATE:
                    new_population[i] = random.randint(0, len(executors) - 1)
            
            population = new_population
        
        # Globale Fitness-History aktualisieren
        GeneticExecutorSelector.best_fitness_history.extend(best_fitness_per_gen)
        GeneticExecutorSelector.avg_fitness_history.extend(avg_fitness_per_gen)
        
        # Wähle das beste Individuum aus der letzten Generation
        fitness_scores = [
            GeneticExecutorSelector.calculate_fitness(executors[idx], capabilities) 
            for idx in population
        ]
        best_idx = population[fitness_scores.index(max(fitness_scores))]
        
        # Aktualisiere Performance-Tracking
        selected_executor = executors[best_idx]
        GeneticExecutorSelector.last_job_times[selected_executor.id] = time.time()
        
        LOG.debug(f"Genetic algorithm selected executor {selected_executor.id} with fitness {max(fitness_scores)}")
        
        return selected_executor
    
    @staticmethod
    def update_performance(executor_id: UUID, success_score: float) -> None:
        """Aktualisiert die Performance-Historie nach Abschluss eines Jobs"""
        if executor_id in GeneticExecutorSelector.performance_history:
            # Gewichteter Durchschnitt (75% alte Performance, 25% neue)
            GeneticExecutorSelector.performance_history[executor_id] = (
                0.75 * GeneticExecutorSelector.performance_history[executor_id] + 
                0.25 * success_score
            )
        else:
            GeneticExecutorSelector.performance_history[executor_id] = success_score
    
    @staticmethod
    def get_fitness_history() -> Dict[str, List[float]]:
        """Gibt die Fitness-Historie zurück"""
        return {
            "best_fitness": GeneticExecutorSelector.best_fitness_history,
            "avg_fitness": GeneticExecutorSelector.avg_fitness_history
        }