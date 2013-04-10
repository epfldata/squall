package sql.estimators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;

public class HardCodedSelectivities {

	private static final double INVALID_SELECTIVITY = -1;
	
	public static double estimate(String queryName, Expression expr) {
		double selectivity = INVALID_SELECTIVITY;
		if (expr instanceof LikeExpression){
			selectivity =  estimate(queryName, (LikeExpression)expr);
		}else if(expr instanceof MinorThan){
			selectivity = estimate(queryName, (MinorThan)expr);
		}
		
		if(selectivity != INVALID_SELECTIVITY){
			return selectivity;
		}else{
			String msg = createErrorMessage(queryName, expr);
			throw new RuntimeException(msg);
		}
	}
	
	private static double estimate(String queryName, LikeExpression like){
		if(queryName.equalsIgnoreCase("TPCH9")){
			return 0.052;	
		}

		//any other case is not yet supported
		return INVALID_SELECTIVITY;
	}
	
	//no constants on both sides; columns within a single table are compared
	private static double estimate(String queryName, MinorThan mt){
		Expression leftExp = mt.getLeftExpression();
		Expression rightExp = mt.getRightExpression();
		
		if (leftExp instanceof Column && rightExp instanceof Column) {
			String rightname = ((Column)rightExp).getColumnName();  
			String leftname = ((Column)leftExp).getColumnName();
			if(queryName.equalsIgnoreCase("TPCH4") || queryName.equalsIgnoreCase("TPCH12")){
				if (rightname.equals("RECEIPTDATE") && leftname.equals("COMMITDATE")) {
					return 0.62;
				}
			}
			if (queryName.equalsIgnoreCase("TPCH12")){
				if (rightname.equals("COMMITDATE") && leftname.equals("SHIPDATE")) {
					return 0.50;
				}	
			}
		}
		
		//any other case is not yet supported
		return INVALID_SELECTIVITY;
	}

	private static String createErrorMessage(String queryName,
			Expression expr) {
		return "The optimizer cannot compute the selectivity of Expression " + expr.toString() + " in query " + queryName + 
				". Try to manually add these information to class HardCodedSelectivities.";
	}	
}