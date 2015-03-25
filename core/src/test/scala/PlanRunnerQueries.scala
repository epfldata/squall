import org.scalatest._

class TestQueries extends FunSuite {
  def runQuery(conf: String): Int = {
    import scala.sys.process._
    import java.io._

    Process(s"./squall_local.sh PLAN_RUNNER $conf", new File("../bin"))!
  }

  test("Hyracks") {
    assertResult(0) {
      runQuery("../test/squall_plan_runner/confs/local/0_01G_hyracks")
    }
  }
}

