package com.ipiecoles.batch.repository;
import com.ipiecoles.batch.model.Commune;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;


public interface CommuneRepository extends JpaRepository<Commune, String> {

    // Query pour récupérer les codes Postaux
    @Query("select count(distinct c.codePostal) from Commune c")
    long countDistinctCodePostal();

    // Query pour récupérer les noms
    @Query("select count(distinct c.nom) from Commune c")
    long countDistinctNom();
}
