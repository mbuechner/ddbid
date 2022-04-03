/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.ddb.labs.ddbid.cronjob;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.ddb.labs.ddbid.cronjob.helpers.DDBQuery;
import de.ddb.labs.ddbid.cronjob.helpers.DDBQuery.SECTOR;
import de.ddb.labs.ddbid.cronjob.helpers.Facets;
import de.ddb.labs.ddbid.service.GitHubService;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DirectMigrationCronJob implements CronJobInterface {

    private final DateTimeFormatter formatterWithThreeDecimals = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.of("Europe/Berlin"));

    @Autowired
    private OkHttpClient httpClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Value(value = "${ddbid.apikey}")
    private String apiKey;

    @Autowired
    private GitHubService gitHub;

    @Autowired
    private DDBQuery ddbQuery;

    private final static List<String> DIRECTMIGRATION_LIST = Arrays.asList(new String[]{
        "00b3c277261b8dea4d65ba98e9c7078b6fd6fece",
        "04397cf7507253e1e8c60a3c4aa0702b7fd22862",
        "051d54ff56114b469d73cc225733c3ce1f07ad4e",
        "05729beeedc53aab338dd27ecaf32592ba981e07",
        "063ba1acfc38868947488c50b45bc7dc76b1174a",
        "0690d713c510c0b28e5e420bcd3bbb4216d0d27c",
        "07bf4694cb41e63f0811b822353bce4c86ce995a",
        "07d4496ee9851dc9e459ed338be1b073e70dc9cd",
        "08d2b7aea1520ccdef1da2297b1885f0211a2658",
        "097e16e25e333540b1422075b478a91dcb9b912f",
        "0a0b6db9269bd555041cb9385974f39e0f7bec18",
        "0a52b9be2645673abf9751305b8fefccbf861b63",
        "0ad439766c51734c3af38a0c0ce9e29a6c7f7956",
        "0b36be539502d7a23e16a01168e55764418e4a58",
        "0bba217e658b78fe4a7c5157e16a6c9202476b63",
        "0c4e89f8aa9c582ac347ca57972fe4ea1bfbac42",
        "0c8d4d40b7218883fbef7649f20c0c60a5be30e5",
        "0d2a235f434c5304177cbc759e2dd19cb908efb6",
        "0d62a9ba772b62e2ad6c102e6e7d1b394ebab24f",
        "0e1a87d2b8d647d58a9b975e19dc2b7c655c3862",
        "0f2f9f8a45ff5b63be3a1144dc0a9f475bc76d3e",
        "0f476d7b203049ff6b620ceb589713fb359e96da",
        "0f5c404b7b17fe29f198cb868acfd13b6ec93ae8",
        "0fa271343ae8e277b69982a7518584adefd6c4d7",
        "0fcc737945b44796324efa669caa06e621e487f9",
        "109380baa5deb4986aa8f56dbc1c2d7bdbdf0484",
        "10a84274db4889b7528022b56a17cdc097f07b91",
        "1148c420a018d09a5fa0c0a76a229f663dc7d591",
        "119eb1493a3a16cb9a921d1001f962ca6f1daee9",
        "11e17eba779b95e057b10e3b0d6e51470eced8ac",
        "1358ee59f2cf7d3a50256bbc61dcf89bc96c0b9c",
        "13afe35154d00920d12bca15a21653b785fd16c0",
        "14145b61139c665e37867cccce77fbbd20519892",
        "155d2fb33b43a523e313140261b4032f475873ec",
        "156a0a4ac5817632ce96eae93e52b4879fd922d5",
        "1688d5c089f613a88be9eb4501a022bd9d49ef1b",
        "16b2a4b768e703f0fb46b0847d9fc028255e895b",
        "16c90b67ddd6d37ddd92e1a2e17decc9404533e1",
        "18554457fab2b3dc53adaa3edfd5b9a8374c70b0",
        "192221524e20ad68c9847adc49969b1ad28e2cc7",
        "1946f9abd557cbf9da183f8f76ab79ff245be139",
        "1a64c1e7f1a01466943fe691497a5a45fba1e293",
        "1af2fc54979b50988db8efc536d7c4b8ada2671e",
        "1b07dad260dee49da6737643c35e717bac449bc7",
        "1b9dcbe616c8dc099aceb063836d778a8d2d3e7c",
        "1caf864b780367332170aa5a9da4a72bb94afa69",
        "1ccd76559c09062b53d2497aea4acb5e4fb4cc22",
        "1d9430f8e38a295afeab5bf697fae0be9c243756",
        "1e161cd06ce85da60ba44a968a5831db0628c2cf",
        "1fc89ee8c0faae3b10b4cae99edcfb78a3ee4970",
        "2056d0c6beafab249dc60768f25bcaaef495022a",
        "207bd00e71d35d6efd49cabc494b032c95af4572",
        "210e6b0f29a7055e84838dbeffe9523705906f4b",
        "2117393e094191eb0f75aeecf14e80a285a67128",
        "215ac1a848fa137bf52dc843a6a4eedaf064c587",
        "21854c2ff9eb807c6460d48b3154889351938e8b",
        "21940a0fa2491ee24b3b8999b07112625c17cfac",
        "21f4eeecbe87416d723cb27ef53e2f1cbf9ebf90",
        "22c1f7bbe18fd526eab56059a8ce4b82f7191227",
        "22ed265ebf58615d2ae66c22fdcfddef5fee781f",
        "23e6a42378a7c1e8933a0a8af71db309bc73862c",
        "23fccfa5bdd58dfcd7c9d7040ff1e9e1bb28c47f",
        "243d4de313fdfaf5d6cf1fc8b7746acbb52fabc4",
        "24784277da53f536efeec12ddaed4e34e055b551",
        "264c4ed4571fc4a48f7006e20e52a53e83fc5340",
        "26f8d0887e63d04f4a7819060aebbf57fc2cb2b0",
        "27862c07c21be006395512894b9bd1d3cfe0ca50",
        "27d4acfe3328c047c6ae8544bc175f878ccdfe53",
        "28f9aac5f7a269bf8c5e3af1694c7267915d7a4a",
        "2952b2fb48894337e527e8f1e21e180ccf491db4",
        "2985205b830bc225eeec7e90dae770575b669d1c",
        "2a3393d7e4db36abcf42d59e9b3f9fcd40d82c42",
        "2b37070d2d56a7eb1e67cb7ca581fbc5a6c32e4a",
        "2bc270ee26242d31652d06296640f1ea7879fb32",
        "2c0c7389f76210a4dc58f03b4127ae15130e9027",
        "2c622b69c060d19b14c69b6f1afcc315155b3562",
        "2c75c3a6bb529b76550ec809893a933cf4138ea5",
        "2db551a23f79bbf6c4591d5f82fcb98236260daf",
        "2e921a0e1e93c290b631c772e7abbb1b30688acd",
        "2f86fe94821fcaf1fcfd83193c935cdc42d5307f",
        "2f912d3af2ea326c0b681bcb59f5342216c1d4c2",
        "2fac2e97bf40fe0fc5a440fd5a843c20da03add2",
        "2fddadb4a6f39a52338e25b8db508e2c5def809b",
        "305673b08f448e4037bd96525acec7e1f0838578",
        "30c8e9a329f8e1fce5adb7577774d2caa6572498",
        "3180a0b40c9b47b271ee69800f473cb6083361f7",
        "318c70c068b3c4a0c19ce58d19516f03506339f6",
        "318e35322444d9af4f8cc851a2c2ad25cb5c1b0a",
        "3194ca16d7f6fbe5da24bd2b36f2b05fcf89abbb",
        "31c6e67482cf6ae6da3dd92d8776dd81338533e2",
        "33e06e66844b361cc80a9ab851537e5a87565ca0",
        "36899088c8302d85aa04d64e5d89c2f7a771dc57",
        "36cf58635c0673d91d2865a762159a6d93f04619",
        "36fe299f9913095db113f0e3e9222a1aad4f2932",
        "379df19d13a273b1abe05ab7a27a33822a8cd0e2",
        "37f6d53c7e260b9e29085b7b8ad504537925dec0",
        "384e73a2340930f21e4f33f5df29156c42bc6629",
        "38ada9850717b9d1924700d3c867280496377521",
        "3ab7e26b63f818004efcde776abe38d59841b8e0",
        "3b3508e8d47a2a963dcfa6241d8c44d2bd8ff512",
        "3be26ddadcb145388f050bdd5f8390e9ff14ef5d",
        "3c785c55bc5e7e8a3c5560f865c79d2db6c614e4",
        "3c948c5722bea455499a986db4f37d311bee1e88",
        "3ce50c0708271a1fcf3d0ef33a0998e26bb53276",
        "3d5de0dffc9326fc9440397dd8d7ea0a921b5c88",
        "3e943965e569f37f6b0e9269a9dfca0c88e9ffce",
        "3f3c627b12b12ae842727e72f790793a9097be72",
        "3fc1477bc38f40bfa7ffea196ccab70539904ea9",
        "40ae378dc6f22ac685f663e4d58bc67ff2ff767b",
        "412eb8c8480e3e519ccd2ea4fcde056be06952c8",
        "417e82c5100fe36a941328414e70ee1daa4d91da",
        "41c3e2c4757f33910dfa29ee65318971f9d28872",
        "41c73e1d4e4466f37af047fe57513c2747bdb6c8",
        "42c5e8e79d7ac42f236c4383ff354429d911ba8d",
        "43029b59af86d333f7e58c5a2f765ad936555041",
        "43496783891e5bd7396c7aa24144e915b7697ef5",
        "44181d7d5754f8a63b33893377e4e8ae2498d9a5",
        "44671bed2cf96c2b0ee16931dc6b8df2a34e5c20",
        "450a65dcf1357cd597ebda5e3facb3ccc513f4ed",
        "4638482515852db8fefdc1bead5255d85afba0ad",
        "468d827c83f10c66c8215fbef4bc631201563e91",
        "4702e7209653c54fde9bd044898e8845ae9d6ba2",
        "473694accf732597a0f6a2e061b3136f47a652d1",
        "47d41d394dc43d846db1b9f5c91c8fee44cf619b",
        "4865fdee5115e9d0af8d2625fd89a99503d32e30",
        "48f6446e5ba8b05b682416b1b5e246869fa1f0de",
        "48f8210f0ee94a81a0136d1de02952c391d6de03",
        "490edfe1ff81c5d93402006c4011d3b920287d21",
        "494b75c01b9fda6b2645b5ae3d23bffb2de072ed",
        "49b9f2c0a56de32edeb50463685cb70fabbb36bf",
        "4a3d724fcf89c309ddd949d42cc866a7301fd8c8",
        "4a4b2978d10fd66d6cc258c41c58ed6d947611d5",
        "4abf51974cf1940d22b1ab6ba4bfe21af79732a4",
        "4cf013fa34c3026ced17cf6fdfad96ce46fc6eec",
        "4d0e2f53b4c81c7dacf7962b3cff5155050d5c05",
        "4d3481c19eb8731fafa4f7e4ba112964b05d2d0e",
        "4d7d5e3aba50dcc2069c21aba30cc6310862bb93",
        "4e843b1ba0aa8d9dd216dcaf9d720fa3e9d1501c",
        "4e9a9d3a192522969bc5143cce6285993ed79f24",
        "4f84c91e3ce2c630cf13a19ed67fbccf8ffb9579",
        "5084cdc66c79c3b6ca5d463d0f985888534f417a",
        "50920f957f7dacff9549e8904fa79051551e76ba",
        "5141f2b80de94d319d3767a4260b07a0523cae85",
        "515e81fea5ac16f21bff800268a2e58f60379d32",
        "5174bc3beb51f8792be29131b228601233dec30d",
        "517e8588cdaa44db6a1ea42155ad63230bd8786f",
        "5190a6dba703009dabdb30cab6746fba21c68740",
        "522651690d41de8f9ad1a65e4f28da8419b3c8e6",
        "5284044b287bcc15cebb2dda2c6e7c3719e0a6af",
        "52a7703748ac2b3bc5bd255049e5426070f30f0f",
        "52ca217e253e80f64a44fb4394c2a6baa4541138",
        "5336b6e0599e1fa4c878e3243c5738f4f9137218",
        "539c07cd80a5f1aed4378e161857d17f52b7bb76",
        "53d5bbb161aa7b44c0521b7024fb3bcb54fb5b1d",
        "5471312e5a25008da8d462ac52967cb002e6aa84",
        "54f1a6668179f3366c03de980343cdb4b6917330",
        "56497d6f35e72ee5343b9c9177a9995680c8cc6b",
        "5656102abb81e234a84afa4ffd3b1ef9e72cfa27",
        "566f15f5c8462c5870600c3ee5933fe96218d4ad",
        "56e54ab32fcf3167407dad353ad16190d28e789c",
        "5846ea6692ca201c3a236861a8d3fb66495f778b",
        "586fc0b3741c9ebb9ecc29e97257d68b5c6051b4",
        "58efe98f9922c2da91ba155bf242f52582217bd0",
        "591627d1485786785740f9bc23ffa7b26f3438b1",
        "593585d7ace896059abedb8d0728db8e57823fe1",
        "5accf68b3937f0d9c30e5cd7e1135fbfe047db58",
        "5af541d3102b7559d30c889242e12349446afb17",
        "5b32997628659a62183ca93e93d43f755e645a97",
        "5c497b63f6f83c8812914e4ee20cba1d176ba23e",
        "5c4f291969d83412c172a831decdbddfaf69261b",
        "5c60f925230339f339bc82696880866c93d90bbe",
        "5d35c7537f9ab13b5265fa3e72df18624d4c6bc1",
        "5da9a8d44e87b2dd817cbbb8c31a93e692e5cb0f",
        "5e3a2c5a157c95622c1182b1e689ba54c0bfff67",
        "5f27f408a4f6f7b5fead14fd419a9ba82b5b44d8",
        "5f2ff8b530b41ae406c625a6081be98f0ad1618f",
        "5f9f8b4182e2a300ecf2e92a18c8dc94decdab9b",
        "5fb9d2d3a985ace3f21a7adaf75a0e2892db11f0",
        "60af45a017f07901d4a6924f93a14e59c863a7ba",
        "61a88a58769b247716098033fa5b81bebfbf90e7",
        "6386b2c5e5de11eaef3c4776a3619d7b318ce6b2",
        "660cc7bdb68e3283cfeef7b797ac16d2afccd539",
        "66e94ba82b835a9f3cb8d1ac64fbff6b7bf8d8cc",
        "68c1dee00bdaf9de3e4d7e6b34306ae5403141a0",
        "68c236ba3fd6d9a1a5c3ed9f020a1a61084dd7c2",
        "690774b7fa119051e7e3783d1eb2baed128fc439",
        "699aaba492d4182f791818dd61ba794f2346ca86",
        "6a895425f3b3e8f8364abe6a3c00f219fde466b2",
        "6b2f871e3ec3da0a1e430a46243dd394827df9e8",
        "6b4b95f0bc04008ee1fb63aac06e50edf75c4e0f",
        "6be39290a6d0656a987050b00805bf5436628c9b",
        "6e11e4c7004b0a7b85997564a72273967a7f5cac",
        "6e5159a623cf871830bfbe54da1f3940016fece0",
        "6e963e98c5b8d9512ca04383d347fbf1d8dfde5c",
        "6ef07411ed5eadbca42f74971bd039911fbc6c57",
        "7023d66eb85e8cec9f12d9e416dd5fcc605bada3",
        "7063f6b31b116f6e81278f419c9155fc03becd0d",
        "70ecc4028e3d51a2f226178336c92a5d6a02d8e5",
        "7138f8884607c6590b73825e1333d5b5b78bb645",
        "719b8dfa92e1b9a18e4aa4ffb597029df3514511",
        "74c6d9fc742c9f2c6922ff42ed439a81bee4afb7",
        "7511ac15523bfc6020cb28302448a2ae5860719a",
        "7554389c96567b93422d06c0aa89f709892eb046",
        "75db3c0c2c0b76b7ab5305baa07a6d46f371d9cf",
        "75ec58af0f6f229c5bcd76c1006ff963ed1ec672",
        "763c9c04b1116f3fdd03734cf7694c0de9c26b10",
        "76ea5dcd9291a60fadfb5cbedd70a54c96739b19",
        "77966022c3dbd092516cd73d2a2fec0d3fcaf50b",
        "77e5079da0a216f30a709f55121c354119b5fa79",
        "783b9dfd91dbe3df6f6fae058c1287a8679d92a6",
        "7890f068ba8ce2087795df4367be5030e2164dbc",
        "79643ce69827e5ce8b379c4d8279cf5ef8450121",
        "79d0434d707d220fe4cd73385cf4e1fe1b451eb1",
        "7a3c0e939d392e60d05ac67b87d462f3eba343d0",
        "7b670186096bc6950bfd7b48e9ee839adf02add2",
        "7c362c69a0e94bb0821726aece0ced746aa495b2",
        "7cd5e5fb43661796e9c62ecfb8c6aac5fa31db3b",
        "7cf4d87fdc89cfaca5a2fd2278614784fd8241da",
        "7d04c1ef9791a8d1a64c1061c3aa4702af6a8341",
        "7d28fbb5026be94b175843653890df08dd7df031",
        "7d3a958de558512f85f52be08c436f2d06c095c5",
        "7dd08c39ac7059a55590b3842165715191f79a5d",
        "7dfdbe471dd6ef2e5002ef3694a4a41a667ab8a3",
        "7e306eeccf7953b49b706055885de422da2496e6",
        "7eb82010b3706f6059fef352df62547aafea634c",
        "7ec9eb27af85620b9c5ba1076b7631667ff6ede4",
        "7f0c09b3e0fedb6a40008affdb0e49a372316122",
        "7f26355043abc182d40fee378a8ad0bee9029dbd",
        "8117b376d50acd7c92f8ca47a786bdcd779c093a",
        "81499bb9d2747d83d2f77cd805e986227934be97",
        "81d3b890543537da32d4cd39320d5f068d50b854",
        "82428c13cf218d6cab0875603b8eae84b3b9bfe1",
        "8275185a810819d13f48482b314e2fb61298e392",
        "8277bad4a6782f6585912660b1f27aa16f767138",
        "833e54865379dd04770fc5349763b02894dbfc35",
        "83edfd7341339f352eb8fdf1cf8491e163c9af12",
        "846a27ed278ad54ea6ef887d4d4d5d429853d4b1",
        "85cc11eb483da320d9f6284455792f1f4ddd4288",
        "861a50ac0fb9aa8126e1ccd34a0f607ce0cc44fc",
        "86b9abb62dd1b3901b8a78db34c91010b6586ffa",
        "86d2621c9bd91d298ecb3ccb2e9e5bcf39fbd212",
        "87796459f8877603baa4e05fe6d204c9d92c495a",
        "87e65ae630216cb72b9275b764600204bd2474a2",
        "87e675237cd7d53ae490ab43c2669811007c40b6",
        "8807429571941cbcfd6d4e681b9073f6b3c98b13",
        "882cf64fcce342c267f4f927d6b6b909e8634341",
        "8955db180d51db23431de451fbd9413c032eba39",
        "8977567814e4426e9589e62dbb476b36440ecb0f",
        "897cce494907b8edf55ab2320a753c62c24b85b3",
        "89caaa323012113aa3ff8c27604ff35bead0f88e",
        "8a2c95b69d39043e76553cce5385ebbe6ee7a2c3",
        "8a472578694864b417c4f84b044f3508b7c7d912",
        "8a65c3e9784e66415d82d9c7f574fcab04b058ae",
        "8aabb7a8f508667d768ec3b7ea808bbc0d8d6499",
        "8acad78c90eff27e1c91abbe600b3f92ce3baf56",
        "8b3946cec2a1a06ebcd1e79b74adb2c274863327",
        "8b95fda52e4ebbde8903d2afa378d4f80e8f6be9",
        "8cce5693fac2b6434e8c66eb02bb1892391ceb94",
        "8d0bd249aff6efd85e9c00ce9075c1d8d850d567",
        "8d5f4bfba2400f1e77ba148e220e975024b59681",
        "8e02b75ac3e72733433425f254305d70b6932100",
        "8f83af90dd72e0f8e22426ef0edf801408eeeea2",
        "90639ae7c49f73ee77b926c08758969a63a089ea",
        "92299e7c7881bae20b2cfdb9d1e1b21b5b1d02da",
        "929afccb450d36cac2316ec0ae7ac8608c011091",
        "9382b2e6989eff1cbce53de1c31ed30f792615b3",
        "95d503039824777dd586a15f8f8423bb4131a1cd",
        "9662dd7a334715b35666e892a803cd0b7442e2bc",
        "967de1010b8933857fe0f1cb8606bccf6115986d",
        "96e2b60f39fa2bb26d7577dcc0b3f27ea4b0d760",
        "974ec95efb607ccc7ae303a0ca6fab50d68f0274",
        "98a54977d1066074aa784174c5eb57bbacd3c363",
        "990f4c733f27618e2898c208dce0fb2e5f950917",
        "998286ba4ea84ba69c0140b03ea562ba2caacf4d",
        "99f35220f8f874f0dbf17dc33bcf4b2b17fa812a",
        "9ae85f212079f120df86ced139d1dd6ae496e460",
        "9cfdfb8975774deb0366ea8d912f861342c9ed3a",
        "9d40f1c7161146a8052b73c742affe396e3268e9",
        "9dba305a329a85113ca09089d30ff28137f3bc30",
        "9e7778454e0a0bc6812fcf38cb37743f2c440acd",
        "9e9ec2444a751fe25b362d0c0b8bc0c02d9f229f",
        "9f23d490a64b24aab40f639e31cf22e2e32e98f3",
        "9fd410652a39857571e4acf18b31adee4c892b5c",
        "9fd89ec5c4f64bbd8917c9a0fc9ca627e06bdcb2",
        "a0b3c5b3f211c7511cc528aadf8dc1a7c3100d3f",
        "a26111b897fd599d1d07454abb6611d25273f317",
        "a2d6c34f3ba55fb6db1ac523720e9252a945e6a6",
        "a38eb5fe75fdf368d04bf18006c5b09851560885",
        "a40630b92757ad957198d559751df646548489f9",
        "a48844d2fffc5c5ef313fe926f22c490448e2487",
        "a6a92a6d0550324a481927c3ff8b7307f6c0f753",
        "a7baf8270e7476b2f1d430c99c538d36a432e3e2",
        "a7f6082ecc3df9c94608956d864f337e0e8fd622",
        "a808106ebbad7d102d1cb887632b025e7230911e",
        "a81548e6828c54c0315e0aef07a6ccb734aee8db",
        "a874fa68df76f1c496e5671056c04f617ce54ff3",
        "a897d11db61bdde27b149d19c041882be7c73a6f",
        "a8a39487354c9ae7f9254f65598dc621568c5df2",
        "a9249c06921be1a485405dd3166410345e345329",
        "a9c1b7f23b33bef2e0d52e6c9353ff846a6e24dc",
        "aa2302e050fba606be021657732f62dd5e2e0697",
        "aa3079a048e15adb8c6884942c2bcd864c7ee800",
        "ab7766efe1d56295ad7039a385d4f540fadbb148",
        "aba7e14da4d87533886b40d80a7f7291b42fc5f7",
        "abd14b67453bb83911097b75b6f9d7ee295ff35a",
        "ac0342cded7fa7bab295d04de0b432d5a760ba2b",
        "ac47ae2b30d420fe537f499d311df64de2ad1f69",
        "ac6bb3c6fc1ebe18b217baa894a03e40fdb6964d",
        "ac8544f3004f57bce3da816ca86915afcf9e8831",
        "ad3eb5344c3a9c90904243dcea8f736d854b2f31",
        "ad84ab38fd6265b57773e03b92eae97161ee5c7c",
        "adae102bd2cc8240676af3285e880c16f69579ad",
        "adf8f15f52ad92e77aba8704513ea8154668dd27",
        "ae3c5182c84ce0449693f109181220c29de71d3a",
        "aed993eaf018b9a2231e3bbd3ecfc8dca81dc3ce",
        "af3332b777ecc53c3ec828ae62b2a66c8398807f",
        "af3ae554a51411f748b96c2169b59422567649ab",
        "b10604ef857acbf17da53b4e9fd9fb5564c535e6",
        "b11fbb6186a3f1b7b3057c68632af56be772cf68",
        "b184086174c747472e085fd87ae7091fef2f635a",
        "b1ab4c880c43a338b1c34a63321793e5ac808369",
        "b224583e414408f50f11f49983fe5c99c79c7398",
        "b2926acedcdbe9cf373784edd516e202ca222ff3",
        "b2e84a28c4f440b3cf2c7c7418e851b13f82a3b6",
        "b3023c3c963a35d28fa0a62c044d8cc312fad6ff",
        "b315a7cc404f326d26742e58bf0e2934bf374d11",
        "b362e7b383d6b0038579c1e8605261b2261dec6b",
        "b36b0ef62f4685d9d7176c4c0dd0aaf6d9a87c5d",
        "b3cb45a015b3d710f0bdd03f669a02cbd20d87d8",
        "b41541753da3ba7d391165f0c76185c22b638787",
        "b5c5a461cf9fd0c80ffb07909d3129acf9d90efc",
        "b610efc8b381c992ef8261fe8829d2097256fee3",
        "b63801d158e96700ad7dfee7021bbacfdc7b375a",
        "b72448b0e57f560f4de425969b1b9ace7fc477dc",
        "b737d506bda5d0adf8edbcd19ea1cf57ef7db621",
        "b7c07d9beb395e19b233f673aae184c0b6288400",
        "b7f8b2404ab4091b1da0f075f787666b4d37bdcb",
        "b908be29a212ef68125e06b55bc5426cbb8dbd67",
        "b9386a19f4deb228830569d804b28b1fd00ea226",
        "b9b70277e183c76dd8fa6fdac3feffe3c60a6d24",
        "ba151aa4be04394313f90a72a96b9f34f4ff700f",
        "ba7f0b396d96e13ed800b6e3c98e0157200f05ab",
        "bb0a9ae7575cfda707aeaf207f309ac9c35b6862",
        "bb2f4eb373e008b22e403c8abba09e1f446b61c9",
        "bb37badf0f7b4529bbbbac4a17f479e8503fb1dc",
        "bb487ea3eb29880bd99256c4cd46bc5963fa85da",
        "bddf9b16dc38d4c7d36f4f9c0176350af17028a5",
        "be200e75b55c96e690922a56e6f0cf37913c1f7b",
        "be3e3eb9685ed18f5db8d020c1c6e356233b7ca1",
        "be7e60b29ff4dd49b45545b7e6f5145ae365dfb1",
        "beb2f6ffd71f6b414cb607059ad8507457eb01fe",
        "bf596a0293ca22461887679d82385346613b7f75",
        "c039e6d603340ea19f618bd53ca538e51870eabf",
        "c057d450f11c14b741f419554b84caaebf879e5f",
        "c067a028c7eef6b33a966baa8d22bf0262aa231b",
        "c09ee3fc244bfef05c4a8b3522c687b1df5eae83",
        "c0c51b35fcaae5e658af1d3cd4ee81d4dae7bf28",
        "c1ca7bc438dd1d502962a6ef75a4b01fdae0c32a",
        "c1f01c65c1d4932d53c30f02206c5d192627990f",
        "c2f3a98af23dd4dba415a9dcad27b4c47eedcd34",
        "c2fefd3630031b2ffd7ed77acab0d1c3398cbbf4",
        "c304a1155fcf3102f0b1dedba5b8b042cf2ecfc3",
        "c377ac13515804683044433b64671030bdcb9526",
        "c3c962948bd7792436b8ca913655bcc5ce4976ed",
        "c4d6fd303dafc1390aeac07403759f1a740ae19f",
        "c59c7f8251ad2aa5061f0700389c5508003741cc",
        "c5df67d4818d2e3134e5e447a6925227e4f7de61",
        "c606076b8d1e7df357e543fc60110c8914992df9",
        "c6dc0d82d71e1eb7c159310005bf602ab4f8464a",
        "c7c48ce6761b335ae36f1e1a5304fd6b5ff7abb5",
        "c890686018e37759077942373aef36ca09391984",
        "c8a2c0a28b7f6cf093ddd0cb25b6d2a71ead1b3f",
        "c8b093d04e80c4b51f20b969c0527b5c8698c076",
        "c9131886a51beb3e80c2e99b93e0edd0c76a40d9",
        "c9499fe49753f98b671778fdf1288c0e2d535863",
        "c94ccdfb1480aeb5fce05adce37c8aa6dca40c84",
        "c9c8abee5cc803c8b70f469a4c4ee45e68397bd4",
        "ca35962c8066126eedacb46e36bd669cc582cfa1",
        "caa53353040617aaf4ec5a68f795f5e4340d4ea7",
        "caadd2758c9761cfb1d8ee1811c596482dae27cb",
        "cb1194c9e36839723c8b423b6ffe7cab9aff28e5",
        "cb2358dc9d329ed6de20ef1da2f54e8b8287344d",
        "cb830aaa709560515f68fa55059415e1fc3728d4",
        "cbbe2272b1d16c68c714637b76a9449893f649d3",
        "cbc0c711cbecfdbcc2ba9bf68570d2feb7a415a4",
        "cceea0f04b8bbb01a5c720312fc4f1e130eb8bb5",
        "cdd6d819bdd244867fcbbb52e0af8c1d59a6e275",
        "ce56bd7a5994446dfa3902fed77963b371ee8a4e",
        "ce7adc0938a265f8b792cedce3d436ab3f906c2b",
        "cf144813e670fac74e51f242c78c9f662b0b591c",
        "cf8f3db0260ea294a7c86a0dcd04628fad635c5c",
        "d0421343949f99b7deddc608cb2231078133a133",
        "d05cced6608c4c7790660d175f8148b99fed2c76",
        "d06bb73eec3ab34f122a956abeb571cdfb69b5c0",
        "d077118193e0a2fda5bc0c5d133acfb6a5e4b37c",
        "d0ee62b0e8715fd0c2d52cd445f813a7a56cb85c",
        "d107a3f7d2d319f2cdd10a38f64fbfd9d4503a33",
        "d12b87b7363641546522663363e6af36050c1568",
        "d15b5982b5b4c62753ddac5dbf5d30b5c3290bf4",
        "d318fd059a21e3801756a738f7dca708ce6e52e6",
        "d3b96edb63e2abd84aad837d73a65103669dd987",
        "d3e02dd107eb7cbcb529a74090aae8d0230acb76",
        "d42b88fcfae71a57d1cb79f8e59aa2adef0c2892",
        "d4814eff19dac43774f6d6264de6fff32e9b0d2a",
        "d4ea2993620eb58e7f2f85f39747f4072fa71faa",
        "d51bf97328cd1caae8986ed62893a9f5458e369e",
        "d5b0bb6dafc713d8000b424b4b92a4cc1e5ad3e1",
        "d5ee73dc53acc6b99772b1ba4c540f4ecf9b4682",
        "d669f45fc28a0fe247be468ebd206bb7b55b39ae",
        "d6c0e3fc0e515717523e0b8f43078c0284a16062",
        "d71ff566fa7cffbb30abf49f7374ecd756e9f8d6",
        "d745c1a4225a376bfd3f9f90eac3c264d7643e7d",
        "d7b2603b2e6dcd123b491e4b916c4e3412030327",
        "d7d0926edbc20423d85bfb700268707b0450fa9c",
        "d7f0d8702ec63a0e5a78a8b504b06d77cb49a6eb",
        "d8085293c3c3f991f6151a2b80dc7f487bf453f2",
        "d82c5ffccc9fbf4f7d1d871f7bc5760664f68620",
        "d8533f0ffea4836a57cb558181c0bcca58ae454d",
        "d885402494a1e98dae4bb07a6a92c2577a777b19",
        "d9d41da9ef4f424d953322a50b47839924661573",
        "d9ff67e54bb3df4eb44e90dbd6d79f76e4872c9f",
        "db4d139067ce6d3002eadbdd13d26fee4fa682ac",
        "db74faf9c3985c39dd998d181ca7af31935fff0c",
        "dba964fd808cfdee7b5f54f3950ef4f3e8b963fc",
        "dd7e18c503c13a3b4a7b322d9a569592f1a7309e",
        "de630227ef0f17f0520b918699a3c010f0ac4160",
        "deba36faa7d837b564591fcb8d973223efd950c6",
        "dec735e4722b719a3c4513b173d1314f8d2b04d3",
        "df0e26310651255930920f21a0c597453d7414cd",
        "df40c5dd3088c1ed5111133accd30c47721aa141",
        "df5d45d7a1d5631405ad9e224b63f9a71368c43e",
        "df90f5ecaa0407cff15164b43887d78298761ae8",
        "dfa1cd5cebd8f03f566cb556c001a2abec06ec77",
        "dfedefb74a83ab6816f00a12402612fb9289d6fb",
        "e1a24b08cdef8fcab8f69880b9f52e32ec179d07",
        "e25f125668b95fb7ad962ab3d34316b06c0a186a",
        "e2c81d60ad0ab6d1abe7e70dff8339489d4def2a",
        "e36e7e356acf2ef0336c7790dc44089e056bca31",
        "e3c5942a8032b1b8ded825a294ed97b17c56bd32",
        "e3c6a64002e354976c0eb9aea3e157884b175e6a",
        "e3e87eef296b2ae0be9317df769e7ef6884c77bc",
        "e454c676cfce86f180cfacace26b22c592da6422",
        "e501cf1de09bc5626ffb4a312bb691b86f7733e7",
        "e511f0f8adcf52edab35ac7731da6ec2193176ca",
        "e52a9ec4d37b98fcc08f6d600b10a84859eb744c",
        "e5d6ba76c10dbc96549430b54a1bc9a114f0cf86",
        "e5f33c5fb4c93dadaec2a5ae69c644429e6df50c",
        "e654c5c5f07244eaeee2411964dd93d682c1f752",
        "e6afe79b0c19103967763b3303401e9ee305d004",
        "e6d0bf72da315b6caf20462cab104450f3826920",
        "e72c6b7c2a2d04dba55d6f057777495136e34e37",
        "e7c3ab1248e129e7ffd8ff400e27454a1a689bf0",
        "e828e07a0be39eacca485e291ec0b9ab54e193ea",
        "e9b7976c03ba15d8984fda20655e6414d0ad72bc",
        "ea84bcbc2c6f4c3d43b88ec79c52b9b11b9c65ea",
        "eb4a49593d6b77ad338db53e53ab16ff805bca9b",
        "eb5995969779dab8fbc2d04bdb32473f321c2d83",
        "ecad3445dcc4609cc7932435ffc307a278af3afb",
        "ed6d8f798232a1990fede2adc16e53592439e657",
        "ede2b734deda2f9d38295fd6675569563cca3ff2",
        "ee72ac5140c818235aab10fbc1337d23df560795",
        "eeb243973be7302b97bd441c5695591106ed5936",
        "eeeaf096184ef7476598cddee79fab9c9eb60b9e",
        "eef28712d347422f5210046356e4c16677749adb",
        "f06e6683acf87366388772c562f4ad315ed24ae4",
        "f194f3fc195510df5000820a0ce75310c22851d6",
        "f1c54283dcece235f625e3b37807dbee83e426e1",
        "f1cf0c191628d3682bfbb3ed77381b9c5e1d5289",
        "f1f18c0d8a5040fb5b650f337bc69fdfedaf142a",
        "f2db2145602ece2cd51dbeceab0db90b7d10ba37",
        "f38fada0f0c711aba2342449228b5bae408ef179",
        "f3bf8986bbde99ae66817c84b5fbdd56f9d16990",
        "f3e168e89b8a0d561c6b5adb5f44547677ba4da9",
        "f3fd7e0ecdab8caa0c555cf13eb64c011b6c6cbc",
        "f48f64c02dd4e4b6c29268406247fabbda851441",
        "f4ccce57c65a81995f2abe6d9487dc304f0c3943",
        "f51c41d991739fb5d346757eea6a74a7800acc18",
        "f72352fcd59bbf5a29bd45cd0cae12c2d2dc8f01",
        "f756fd34199ec293beeded03607b050f3a1dabab",
        "f79770194456a5bc501caa7222228f55d52d4d16",
        "f7d89e0515fccce35d9517f165b745504ea9ec21",
        "f7fd54f66c1f798b78f5dcd8556d55d37e946c55",
        "f83f63f18da1a647f547aa2265a09787197efdba",
        "f865724077aee274e9a0549299d7d2abc5ace4f2",
        "f89925df3531d350abd9f177098c4bde5dbd5cf3",
        "f8f4fb457f549e6382378e905aa79a780a9bf104",
        "f9486a816bb61b5826d04d0fc364c7927d3983d1",
        "fa0e57bb7d627af9b498af17869aea347d4f988c",
        "fa4865324d6a34a08ff267a4df7649706236dffe",
        "fa78154f919437a83daef19d0c57a032fd1b9437",
        "fae30d7ab200ea82cc6b699c7028c93f0d54922d",
        "fb181d9b37a767bff09d12499613144e38febf61",
        "fb237ae743fd911fee28479861e5d10a1b0e3c42",
        "fb9a6c6975e1504451ff8ed8efa1702d8423bed4",
        "fbb512d63b58a41a1b1720ba2b650ee69519de9f",
        "fbf9d82d7e64bb8cb4f8e24086f6d319aaefcf21",
        "fd0f639ee0966efe6b8af18ebc25300c4817a607",
        "fd2ff1f24b1b5461a5054dc250788efa14837f21",
        "fe862a780aedaeb07a8a7437a329ec91f5a7c071",
        "ff378b86638ed62b1392bee7c8b09766d6b117fc",
        "ff9fb847876fd8c7ffb6d5fce6cd975a15740f8a",
        "ffabcf0dbc116217b1421bd02f9534c83bac24b4"
    });

    public DirectMigrationCronJob(@Value(value = "${ddbid.cron.directmigration}") String scheduledPattern) {
        log.info("{} is scheduled at {}", getClass().getName(), scheduledPattern);
    }

    @Override
    public void run() {
        try {
            schedule();
        } catch (Exception e) {
            log.error("{}", e.getMessage());
        }

    }

    @Override
    @Scheduled(cron = "${ddbid.cron.directmigration}")
    @Retryable(value = {Exception.class}, maxAttemptsExpression = "${ddbid.cron.retry.maxAttempts}", backoff = @Backoff(delayExpression = "${ddbid.cron.retry.delay}"))
    public void schedule() throws Exception {

        log.info("Start to make new direct migartion list...");
        final File outputTempFile = File.createTempFile("directmigration-", ".csv");

        final CSVFormat csvFormat = CSVFormat.Builder.create()
                .setHeader("dataset_id",
                        "dataset_label",
                        "provider_ddb_id",
                        "provider_id",
                        "provider_name",
                        "provider_state",
                        "provider_sector",
                        "supplier_ddb_id",
                        "supplier_id",
                        "supplier_name",
                        "aggregator_ddb_id",
                        "aggregator_id",
                        "aggregator_name",
                        "count",
                        "ingest_id",
                        "metadata_format",
                        "ingest_complete_date")
                .setDelimiter(',')
                .setQuote('"')
                .setQuoteMode(QuoteMode.ALL)
                .setRecordSeparator("\n")
                .setIgnoreEmptyLines(false)
                .setAllowMissingColumnNames(true)
                .setAllowDuplicateHeaderNames(true)
                .build();

        try (final FileOutputStream os = new FileOutputStream(outputTempFile); final CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(os, StandardCharsets.UTF_8), csvFormat)) {

            // with BOM for Excel
            os.write(0xef);
            os.write(0xbb);
            os.write(0xbf);

            ddbQuery.init();
            ddbQuery.addFacetValue(DDBQuery.FACET.DATASET_ID);
            ddbQuery.run();
            List<String> datasetIdList = ddbQuery.getFacetValues().get(DDBQuery.FACET.DATASET_ID.toString());

            for (String datasetId : datasetIdList) {

                // revision_id query
                final DDBQuery query02 = ddbQuery.init()
                        .addSearchValue(DDBQuery.FACET.DATASET_ID.toString() + ":" + datasetId)
                        .addFacetValue(DDBQuery.FACET.PROVIDER_ID);
                query02.run();

                final List<String> providerList = new ArrayList<>();

                for (Facets f : query02.getFacets()) {
                    if (f.getField().equals(DDBQuery.FACET.PROVIDER_ID.toString())) {
                        for (Map.Entry<String, Integer> entry : f.getFacetValues().entrySet()) {
                            if (entry.getKey().length() == 32) {
                                providerList.add(entry.getKey());
                            }
                        }
                    }
                }

                for (String provider_id : providerList) {

                    // revision_id query
                    final DDBQuery query03 = ddbQuery.init()
                            .addSearchValue(DDBQuery.FACET.DATASET_ID.toString() + ":" + datasetId)
                            .addSearchValue(DDBQuery.FACET.PROVIDER_ID.toString() + ":" + provider_id)
                            .addFacetValue(DDBQuery.FACET.LAST_UPDATE)
                            .addFacetValue(DDBQuery.FACET.SOURCE_FORMAT)
                            .addFacetValue(DDBQuery.FACET.SUPPLIER_ID)
                            .addFacetValue(DDBQuery.FACET.DATASET_LABEL)
                            .addFacetValue(DDBQuery.FACET.INGEST_ID);
                    query03.run();

                    final NavigableMap<Date, Integer> sortedDates = new TreeMap<>();

                    for (Facets f : query03.getFacets()) {
                        if (f.getField().equals(DDBQuery.FACET.LAST_UPDATE.toString())) {
                            for (Map.Entry<String, Integer> entry : f.getFacetValues().entrySet()) {
                                sortedDates.put(parseDate(entry.getKey()), entry.getValue());
                            }
                        }
                    }

                    // search Provider Information
                    log.debug("Getting Provider (" + provider_id + ") information...");
                    String provider_name = "";
                    String provider_local_id = "";
                    String provider_state = "";
                    String provider_sector = "";

                    final Request request = new Request.Builder().url(CronJob.API + "/items/" + provider_id)
                            .addHeader("Accept", "application/json")
                            .addHeader("Authorization", "OAuth oauth_consumer_key=\"" + apiKey + "\"")
                            .build();
                    try (final Response response = httpClient.newCall(request).execute()) {
                        if (response.isSuccessful()) {
                            final JsonNode rootNode = objectMapper.readTree(response.body().byteStream());
                            provider_name = rootNode.get("provider-info").get("provider-name").asText("");
                            provider_local_id = rootNode.get("provider-info").get("provider-item-id").asText("");
                            provider_state = rootNode.get("provider-info").get("provider-state").asText("");
                            provider_sector = SECTOR.forShortName(rootNode.get("view").get("cortex-institution").get("sector").asText("")).toString();
                        }
                    }

                    final String supplier_local_id = query03.getFacetValues().get(DDBQuery.FACET.SUPPLIER_ID.toString()).get(0);
                    final String supplier_id = calculateDdbId("www_fiz-karlsruhe_de", supplier_local_id);

                    log.debug("Getting Supplier (" + supplier_id + ") information...");
                    String supplier_name = "";
                    final Request request2 = new Request.Builder().url(CronJob.API + "/items/" + supplier_id)
                            .addHeader("Accept", "application/json")
                            .addHeader("Authorization", "OAuth oauth_consumer_key=\"" + apiKey + "\"")
                            .build();
                    try (final Response response = httpClient.newCall(request2).execute()) {
                        if (response.isSuccessful()) {
                            final JsonNode rootNode = objectMapper.readTree(response.body().byteStream());
                            supplier_name = rootNode.get("provider-info").get("provider-name").asText("");

                        }
                    }
                    final ZonedDateTime tmp = ZonedDateTime.ofInstant(sortedDates.lastKey().toInstant(), ZoneId.of("UTC"));
                    final ZonedDateTime date = ZonedDateTime.of(tmp.getYear(), tmp.getMonthValue(), tmp.getDayOfMonth(), tmp.getHour(), tmp.getMinute(), tmp.getSecond(), tmp.getNano(), ZoneId.of("Europe/Berlin"));
                    printer.printRecord(
                            datasetId, // "dataset_id",
                            DIRECTMIGRATION_LIST.contains(datasetId) ? "Direktmigration" : trimSquaredBrackets(query03.getFacetValues().get(DDBQuery.FACET.DATASET_LABEL.toString()).toString()), // "dataset_label",
                            provider_id, // "provider_ddb_id",
                            "'" + provider_local_id, // "provider_id",
                            provider_name, // "provider_name",
                            provider_state, // "provider_state",
                            provider_sector, // "provider_sector",
                            supplier_id, // "supplier_ddb_id",
                            "'" + trimSquaredBrackets(query03.getFacetValues().get(DDBQuery.FACET.SUPPLIER_ID.toString()).toString()), // "supplier_id",
                            supplier_name, // "supplier_name",
                            "", // "aggregator_ddb_id",
                            "", // "aggregator_id",
                            "", // "aggregator_name",
                            String.valueOf(query03.getNumberOfResults()), // "count",
                            trimSquaredBrackets(query03.getFacetValues().get(DDBQuery.FACET.INGEST_ID.toString()).toString()), // "ingest_id",
                            trimSquaredBrackets(query03.getFacetValues().get(DDBQuery.FACET.SOURCE_FORMAT.toString()).toString()), // "metadata_format",
                            !sortedDates.isEmpty() ? formatterWithThreeDecimals.format(date) : "" // "ingest_complete_date"
                    );
                }
            }
        }
        //commit and push
        gitHub.commit(outputTempFile.toPath());
        if (outputTempFile.delete()) {
            outputTempFile.deleteOnExit();
        }

        log.info("Direct migration list created and added to GitHub.");
    }

    public static String trimSquaredBrackets(String in) {
        if (in.startsWith("[")) {
            in = in.substring(1);
        }
        if (in.endsWith("]")) {
            in = in.substring(0, in.length() - 1);
        }
        return in;
    }

    public static Date parseDate(String dateString) throws ParseException {

        final String datePatter01 = "\\d{2}\\-\\d{2}\\-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}";
        final String datePatter02 = "(?:[1-9]\\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:Z|[+-][01]\\d[0-5]\\d)";

        final Pattern p01 = Pattern.compile(datePatter01);
        final Matcher m01 = p01.matcher(dateString);

        if (m01.matches()) {
            SimpleDateFormat parser = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
            return parser.parse(dateString);
        }

        final Pattern p02 = Pattern.compile(datePatter02);
        final Matcher m02 = p02.matcher(dateString);
        if (m02.matches()) {
            SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            return parser.parse(dateString);
        }

        return null;
    }

    public static String calculateDdbId(String providerID, String providerItemId) throws NoSuchAlgorithmException {
        final byte[] input = (providerID + providerItemId).getBytes(Charset.forName("UTF-8"));
        return new String(base32Encode(sha1Hash(input)));
    }

    private static byte[] base32Encode(byte[] input) {
        final Base32 base32 = new Base32();
        return base32.encode(input);
    }

    private static byte[] sha1Hash(byte[] input) throws NoSuchAlgorithmException {
        final MessageDigest mDigest = MessageDigest.getInstance("SHA1");
        return mDigest.digest(input);
    }
}
