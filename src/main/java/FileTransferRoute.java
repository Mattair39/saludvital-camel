import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.main.Main;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class FileTransferRoute extends RouteBuilder {

    private static final List<String> COLUMNAS = Arrays.asList(
            "patient_id", "full_name", "appointment_date", "insurance_code");

    private static final Set<String> SEGUROS = Set.of("IESS", "PRIVADO", "NINGUNO");

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        main.configure().addRoutesBuilder(new FileTransferRoute());
        main.run();
    }

    @Override
    public void configure() throws Exception {

        from("file:d:/Cursos/IntegracionSistemas/saludvital-camel/input?antInclude=*.csv&delete=true")
                .log("[ADMISIONES] Archivo detectado: ${file:name}")
                .process(new ValidadorCSV())
                .wireTap("direct:registrarLog")
                .choice()
                .when(exchangeProperty("esValido").isEqualTo(true))
                .log("VALIDO   | ${file:name} -> output + archive")
                .to("file:d:/Cursos/IntegracionSistemas/saludvital-camel/output")
                .setHeader("CamelFileName",
                        simple("${file:name.noext}_${date:now:yyyy-MM-dd_HHmmss}.csv"))
                .to("file:d:/Cursos/IntegracionSistemas/saludvital-camel/archive")
                .log("Archivado: ${header.CamelFileName}")
                .otherwise()
                .log("INVALIDO | ${file:name} | ${exchangeProperty.motivoRechazo}")
                .to("file:d:/Cursos/IntegracionSistemas/saludvital-camel/error")
                .end();

        from("direct:registrarLog")
                .transform().simple(
                        "${date:now:yyyy-MM-dd HH:mm:ss} | Archivo: ${header.CamelFileName}"
                                + " | Valido: ${exchangeProperty.esValido}"
                                + " | Motivo: ${exchangeProperty.motivoRechazo}"
                                + "${sys.line.separator}")
                .to("file:d:/Cursos/IntegracionSistemas/saludvital-camel/logs"
                        + "?fileName=admisiones-${date:now:yyyy-MM-dd}.log"
                        + "&fileExist=Append&charset=UTF-8");
    }

    static class ValidadorCSV implements Processor {

        @Override
        public void process(Exchange exchange) throws Exception {
            String contenido = exchange.getIn().getBody(String.class);

            if (contenido == null || contenido.isBlank()) {
                invalido(exchange, "Archivo vacío");
                return;
            }

            String[] lineas = contenido.split("\\r?\\n");

            String[] cols = lineas[0].trim().toLowerCase().split(",");
            for (String col : COLUMNAS) {
                boolean ok = false;
                for (String c : cols) {
                    if (c.trim().equals(col)) {
                        ok = true;
                        break;
                    }
                }
                if (!ok) {
                    invalido(exchange, "Encabezado invalido: falta columna " + col);
                    return;
                }
            }

            int iId = idx(cols, "patient_id");
            int iName = idx(cols, "full_name");
            int iDate = idx(cols, "appointment_date");
            int iSeg = idx(cols, "insurance_code");

            for (int i = 1; i < lineas.length; i++) {
                String linea = lineas[i].trim();
                if (linea.isEmpty())
                    continue;

                String[] f = linea.split(",", -1);
                if (f.length < cols.length) {
                    invalido(exchange, "Fila " + i + ": columnas insuficientes");
                    return;
                }

                if (vacio(f[iId])) {
                    invalido(exchange, "Fila " + i + ": patient_id vacio");
                    return;
                }
                if (vacio(f[iName])) {
                    invalido(exchange, "Fila " + i + ": full_name vacio");
                    return;
                }
                if (vacio(f[iDate])) {
                    invalido(exchange, "Fila " + i + ": appointment_date vacio");
                    return;
                }
                if (vacio(f[iSeg])) {
                    invalido(exchange, "Fila " + i + ": insurance_code vacio");
                    return;
                }

                try {
                    LocalDate.parse(f[iDate].trim(), FMT);
                } catch (DateTimeParseException e) {
                    invalido(exchange, "Fila " + i + ": fecha invalida (" + f[iDate].trim() + ")");
                    return;
                }

                if (!SEGUROS.contains(f[iSeg].trim().toUpperCase())) {
                    invalido(exchange, "Fila " + i + ": seguro invalido (" + f[iSeg].trim() + ")");
                    return;
                }
            }

            exchange.setProperty("esValido", true);
            exchange.setProperty("motivoRechazo", "N/A");
        }

        private void invalido(Exchange ex, String motivo) {
            ex.setProperty("esValido", false);
            ex.setProperty("motivoRechazo", motivo);
        }

        private boolean vacio(String v) {
            return v == null || v.trim().isEmpty();
        }

        private int idx(String[] arr, String val) {
            for (int i = 0; i < arr.length; i++)
                if (arr[i].trim().equals(val))
                    return i;
            return -1;
        }
    }
}
