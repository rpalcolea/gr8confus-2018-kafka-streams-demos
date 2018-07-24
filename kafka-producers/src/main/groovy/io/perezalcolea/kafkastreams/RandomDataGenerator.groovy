package io.perezalcolea.kafkastreams

class RandomDataGenerator {

    private static final List<String> PUBLISHERS = Arrays.asList(
            "Manning Publications",
            "O'Reilly Media",
            "No Starch Press",
            "Addison-Wesley Signature Series",
            "Packt Publishing",
            "Prentice Hall",
            "Pragmatic Bookshelf",
            "Springer"
    )

    private static final List<String> LANGUAGES  = Arrays.asList(
            "English",
            "Spanish",
            "Japanese",
            "French",
            "German",
            "Portuguese"
    )

    private static final Random rand = new Random()

    static Double randomPrice() {
        (10.00 + (69.99 - 10.00) * rand.nextDouble()).round(2)
    }

    static Integer randomQuantity() {
        rand.nextInt((10 - 1) + 1) + 1
    }

    static Long randomLong() {
        (Long) (Math.abs(rand.nextInt() % Integer.MAX_VALUE) + 1)
    }

    static String randomPublisher() {
        return PUBLISHERS.get(rand.nextInt(PUBLISHERS.size()))
    }

    static String randomLanguage() {
        return LANGUAGES.get(rand.nextInt(LANGUAGES.size()))
    }
}
