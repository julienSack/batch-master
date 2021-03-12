package com.ipiecoles.batch.dto;

public class CommuneCSV {
    private String codeInsee;
    private String nom;
    private String codePostal;
    private String ligne5;
    private String libelleAcheminement;
    private String coordonneesGps;

    public String getCodeInsee() {
        return codeInsee;
    }

    public void setCodeInsee(String codeInsee) {
        this.codeInsee = codeInsee;
    }

    public String getNom() {
        return nom;
    }

    public void setNom(String nom) {
        this.nom = nom;
    }

    public String getCodePostal() {
        return codePostal;
    }

    public void setCodePostal(String codePostal) {
        this.codePostal = codePostal;
    }

    public String getLigne5() {
        return ligne5;
    }

    public void setLigne5(String ligne5) {
        this.ligne5 = ligne5;
    }

    public String getLibelleAcheminement() {
        return libelleAcheminement;
    }

    public void setLibelleAcheminement(String libelleAcheminement) {
        this.libelleAcheminement = libelleAcheminement;
    }

    public String getCoordonneesGps() {
        return coordonneesGps;
    }

    public void setCoordonneesGps(String coordonneesGps) {
        this.coordonneesGps = coordonneesGps;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CommuneCSV{");
        sb.append("codeInsee='").append(codeInsee).append('\'');
        sb.append(", nom='").append(nom).append('\'');
        sb.append(", codePostal='").append(codePostal).append('\'');
        sb.append(", ligne5='").append(ligne5).append('\'');
        sb.append(", libelleAcheminement='").append(libelleAcheminement).append('\'');
        sb.append(", coordonneesGps='").append(coordonneesGps).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
