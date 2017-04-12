/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.processor.entity;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
 
@Entity
@Table(name = "Processo")
public class Processo {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long id;
    private int processId;
    private int processato;
    private String testo;
 
    public Processo() {
        // nop
    }
 
    public Processo(String testo, int processId, int processato) {
        this.testo = testo;
        this.processId = processId;
        this.processato = processato;
    }

    public long getId() {
        return id;
    }

    public int getProcessId() {
        return processId;
    }

    public int getProcessato() {
        return processato;
    }

    public String getTesto() {
        return testo;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setProcessId(int processId) {
        this.processId = processId;
    }

    public void setProcessato(int processato) {
        this.processato = processato;
    }

    public void setTesto(String testo) {
        this.testo = testo;
    }
     
    @Override
    public String toString() {
        return String.format("Processo[id=%d, testo='%s', processId='%d' ,processato='%d']", id, testo, processId, processato);
    }
}
