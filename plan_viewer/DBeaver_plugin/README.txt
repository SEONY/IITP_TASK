DBeaver GOLDILOCKS plugin

DBeaver 빌드 시 JDK 11 이상, Maven 3.6 이상이 필요하다.

== 빌드 ==

1. dbeaver 소스를 다운 받는다.

% git clone https://github.com/dbeaver/dbeaver.git dbeaver

2. org.jkiss.dbeaver.ext.goldilocks 디렉토리를 다운 받은 dbeaver 의 plugins 디렉토리에 복사한다.

% cp -r org.jkiss.dbeaver.ext.goldilocks dbeaver/plugins

3. diff 파일을 적용한다.

% git apply plugin.diff

4. build

% cd dbeaver
% mvn package

빌드 후 plugin jar 는 product/community/target/products/org.jkiss.dbeaver.core.product/linux/gtk/x86_64/dbeaver/plugins 경로에 생성된다.

생성된 jar 파일 뒤에 빌드 날짜가 추가되어 있다.

== plugin 개발 ==

다음 URL 를 참고해 eclipse 개발 환경을 설정할 수 있다.

https://dbeaver.com/docs/wiki/Develop-in-Eclipse/

== plugin 설치 ==

빌드 날짜가 있는 jar 파일의 이름은 아래 예제와 다를 수 있다.

아래 예제에서 빌드 후 생성된 jar 파일의 이름에 맞게 수정하여 사용한다.

1. GOLDILOCKS plugin jar 를 dbeaver 가 설치된 디렉토리의 plugins 디렉토리에 복사한다.

% cd dbeaver
% cp org.jkiss.dbeaver.ext.goldilocks_1.0.0.202208171013.jar plugins

2. GOLDILOCKS plugin 정보를 configuration/org.eclipse.equinox.simpleconfigurator/bundles.info 에 추가한다.

org.jkiss.dbeaver.ext.goldilocks,1.0.0.202208171013,plugins/org.jkiss.dbeaver.ext.goldilocks_1.0.0.202208171013.jar,4,false

3. 이미 dbeaver 가 실행된 적이 있으면 이전에 캐시된 데이터를 삭제한다.

% rm -rf configuration/org.eclipse.core.runtime/.extraData.1
% rm -rf ~/.local/share/DBeaverData/

4. dbeaver 를 실행한다.

== 참고 ==

티켓 #4893 을 참고한다.
