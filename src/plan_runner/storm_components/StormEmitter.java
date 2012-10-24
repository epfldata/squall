package plan_runner.storm_components;

public interface StormEmitter{
    public String getName();
    public String[] getEmitterIDs();

    public String getInfoID();
}