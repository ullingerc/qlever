
#include <absl/strings/str_join.h>
#include <gmock/gmock.h>
#include <util/geo/Geo.h>

#include <range/v3/numeric/accumulate.hpp>
#include <stdexcept>

#include "GeometryInfoTestHelpers.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/GTestHelpers.h"
#include "util/GeoConverters.h"
#include "util/Timer.h"
#include "util/geo/Collection.h"
#include "util/geo/Geo.h"

namespace {

using namespace ad_utility;
using namespace geoInfoTestHelpers;

TEST(GeometryInfoTest, areaperf) {
  using namespace ad_utility::detail;
  const std::string inputFile = "inputy.txt";
  const std::string outputFile = "outputy.txt";

  std::ifstream in(inputFile);
  std::ofstream out(outputFile);

  if (!in.is_open() || !out.is_open()) return;

  std::string line;
  size_t cnt = 0;
  while (std::getline(in, line)) {
    ++cnt;
    if (cnt == 1) continue;  // header
    std::istringstream iss(line);
    std::string part1, part2;

    if (std::getline(iss, part1, '\t') && std::getline(iss, part2)) {
      ad_utility::Timer x{ad_utility::Timer::Started};

      auto a = GeometryInfo::getMetricArea(part2);

      auto t = x.msecs().count();
      out << part1 << '\t' << t << "\t"
          << (a.has_value() ? a.value().area() : -1.0) << '\n';
    }
    if (cnt % 100'000 == 0) {
      out << std::flush;
    }
  }
  // const auto parsed = getGeometryOfTypeOrThrow<MultiPolygon<CoordType>>(
  //     litRealWorldMultiPolygonHoleIntersection);
  // ad_utility::Timer x{ad_utility::Timer::Started};

  // for (size_t i = 0; i < 1'000'000; ++i) {
  //   // computeMetricArea(ParsedWkt{parsed});
  //   //
  //   GeometryInfo::fromWktLiteral(litRealWorldMultiPolygonHoleIntersection);
  //   // GeometryInfo::fromWktLiteral(litSmallRealWorldPolygon1);
  //   // GeometryInfo::fromWktLiteral(litShortRealWorldLine);
  // }

  // std::cout << x.msecs().count() << std::endl;
}

}  // namespace

/*
curl https://qlever.dev/api/osm-planet -H "Accept: text/tab-separated-values" -o
geoms -H "Content-type: application/sparql-query" --data 'PREFIX geof:
<http://www.opengis.net/def/function/geosparql/> PREFIX geo:
<http://www.opengis.net/ont/geosparql#> SELECT * WHERE { ?x geo:asWKT ?geometry
. FILTER(!ql:isGeoPoint(?geometry)) }'
*/
