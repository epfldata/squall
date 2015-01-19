package ch.epfl.data.plan_runner.storm_components;

public interface StormEmitter {
    public String[] getEmitterIDs();

    public String getInfoID();

    public String getName();
}