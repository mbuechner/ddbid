/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.ddb.labs.ddbid.cronjobs;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/**
 *
 * @author buechner
 */
@Slf4j
public class DumpCorrector {

    private final static String INPUT_FILE = "D:\\GitHub\\ddbid\\data\\dumps\\item\\old\\2022-03-13.csv.gz";
    private final static String OUTPUT_FILE = "D:\\GitHub\\ddbid\\data\\dumps\\item\\2022-03-13.csv.gz";

    public static void main(String[] args) throws IOException {
        try (final InputStream fileStream = new FileInputStream(INPUT_FILE); final InputStream gzipStream = new GZIPInputStream(fileStream); final InputStreamReader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8); final OutputStream os = Files.newOutputStream(Path.of(OUTPUT_FILE), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE); final OutputStreamWriter ow = new OutputStreamWriter(new GZIPOutputStream(os), StandardCharsets.UTF_8); final BufferedWriter bw = new BufferedWriter(ow); final CSVPrinter csvPrinter = new CSVPrinter(bw, CSVFormat.DEFAULT.withHeader("timestamp", "id", "status", "provider_item_id", "dataset_id", "label", "provider_id", "sector_fct", "supplier_id"))) {

            final Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(decoder);

            for (final CSVRecord record : records) {
                final Map<String, String> map = new LinkedHashMap<>();
                /// FROM: timestamp,id,status,dataset_id,label,provider_id,supplier_id
                // TO: timestamp,id,status,provider_item_id,dataset_id,label,provider_id,sector_fct,supplier_id
                map.put("timestamp", record.get("timestamp"));
                map.put("id", record.get("id"));
                map.put("status", record.get("status"));
                if (record.isMapped("provider_item_id")) {
                    map.put("provider_item_id", record.get("provider_item_id"));
                    log.info("This fump already have provider_item_id");
                } else {
                    map.put("provider_item_id", "");
                }
                map.put("dataset_id", record.get("dataset_id"));
                map.put("label", record.get("label"));
                map.put("provider_id", record.get("provider_id"));

                if (record.isMapped("sector_fct")) {
                    map.put("sector_fct", record.get("sector_fct"));
                    log.info("This dump already have provider_item_id.");
                } else {
                    switch (record.get("provider_id")) {
                        case "[265BI7NE7QBS4NQMZCCGIVLFR73OCOSL, 00014072, X6VKVOM5HGHDIQX36BI3ZKWROZTN74UX]":
                            map.put("sector_fct", "sec_02");
                            break;
                        case "[2Q37XY5KXJNJE5MV6SWP3UKKZ6RSBLK5, 00012008]":
                            map.put("sector_fct", "sec_02");
                            break;
                        case "[2RBJRLJLDGCQHRDTFSUQ6UHVRRVTJRCH, oid1488275186543, LL7CDI64JH5L5GXNLU53D25TIM6PL2SI, KXX5FA2GGOI6O3XS3JO6G2WDG5LIPNWB, XDNCPF2LBDI5VCSGTHZKY7RINDXKCMJX]":
                            map.put("sector_fct", "sec_04");
                            break;
                        case "[3NW7MGKSV3WGREW62YG54LJYKSNPMPMV, 00000371]":
                            map.put("sector_fct", "sec_01");
                            break;
                        case "[AHIKGV2VQJCTI63BAIGBDSB3J3MLASOS, oid1523956740946, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]":
                            map.put("sector_fct", "sec_06");
                            break;
                        case "[BZVTR553HLJBDMQD5NCJ6YKP3HMBQRF4, 00050009, X6VKVOM5HGHDIQX36BI3ZKWROZTN74UX]":
                            map.put("sector_fct", "sec_02");
                            break;
                        case "[CJY7MSLPOPB7FTPC7JM5K2GGM5PBGLYI, 99900890, 265BI7NE7QBS4NQMZCCGIVLFR73OCOSL]":
                            map.put("sector_fct", "sec_05");
                            break;
                        case "[DZSR6QYESO5Z25FNKUDODHOPRPBNWNMA, oid1526289503911, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]":
                            map.put("sector_fct", "sec_06");
                            break;
                        case "[EGCA66RQCQZMVMLIFI74MXN4PU2Q6AF3, oid1558946268517]":
                            map.put("sector_fct", "sec_06");
                            break;
                        case "[HUO4N7TCVYWGADSRSVDFUJAQEF4OJHXF, oid1526298063398, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]":
                            map.put("sector_fct", "sec_06");
                            break;
                        case "[I6OHYTLAP2UVCMPZS6IUAN6MMBKFDPSZ, oid1523953248808, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]":
                            map.put("sector_fct", "sec_06");
                            break;
                        case "[INLVDM4I3AMZLTG6AE6C5GZRJKGOF75K, 00005846, X6VKVOM5HGHDIQX36BI3ZKWROZTN74UX]":
                            map.put("sector_fct", "sec_02");
                            break;
                        case "[JJUO42747XGNHECFH7ZFVLVQYPXS4ZBK, oid1470131600302, 265BI7NE7QBS4NQMZCCGIVLFR73OCOSL]":
                            map.put("sector_fct", "sec_05");
                            break;
                        case "[JKXOBCEL27PNRZPTRDYWYXVI3E5KHMX2, 00017154, X6VKVOM5HGHDIQX36BI3ZKWROZTN74UX]":
                            map.put("sector_fct", "sec_02");
                            break;
                        case "[MGWFPAWRGVKJ3YVWDOYVLRJQG2CKCLIC, oid1471510992314]":
                            map.put("sector_fct", "sec_01");
                            break;
                        case "[N2JJLO6NUJRVPUKRJAZVQRAC6JUP2ATP, oid1523956195060, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]":
                            map.put("sector_fct", "sec_06");
                            break;
                        case "[R2354GQ5RX6BQQB7RVF6CKRKZT24JEPP, 00050315, U3OFZLW5PNNYI54ZVBLJSCMWIBJ2T5ZU, XDNCPF2LBDI5VCSGTHZKY7RINDXKCMJX]":
                            map.put("sector_fct", "sec_06");
                            break;
                        case "[R2354GQ5RX6BQQB7RVF6CKRKZT24JEPP, 00050315, U3OFZLW5PNNYI54ZVBLJSCMWIBJ2T5ZU]":
                            map.put("sector_fct", "sec_06");
                            break;
                        case "[SAZUM7BIJH2V7RD4VSFSCZO67R4E3SNA, oid1523957047471, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]":
                            map.put("sector_fct", "sec_06");
                            break;
                        case "[TJPSHRLGAEG4CVXBL3ZVMWCDEOSEV2OV, 99900812, KXX5FA2GGOI6O3XS3JO6G2WDG5LIPNWB, X6VKVOM5HGHDIQX36BI3ZKWROZTN74UX]":
                            map.put("sector_fct", "sec_02");
                            break;
                        case "[VS424HF5PDIIP6JGX77KXU27RFTTF4GS, oid1526292408684, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]":
                            map.put("sector_fct", "sec_06");
                            break;
                        case "[XYMQPA4OHAYDDFYWHV6Q4RFUIISTLQJV, 00000896]":
                            map.put("sector_fct", "sec_01");
                            break;
                        case "[ZUSXA5RDTYUYRQ5DWSIOL2TXHV62R47F, 00008976]":
                            map.put("sector_fct", "sec_03");
                            break;
                        default:
                            map.put("sector_fct", "");
                            break;
                    }
                }
                map.put("supplier_id", record.get("supplier_id"));
                csvPrinter.printRecord(map.values());
            }
        }
    }
}
